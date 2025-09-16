# app.py
from flask import Flask, render_template, request, jsonify, Response
from flask_cors import CORS
from datetime import datetime, timedelta, date
import pytz
from sqlalchemy import func, inspect, text as sqltext
from decimal import Decimal, ROUND_HALF_UP, ROUND_FLOOR, getcontext

import os, time, requests, io, csv
from typing import Optional, List, Dict, Any, Tuple

from models import (
    db, Registration, MonthlyReturn, MonthlyResult, Distribution,
    UnitBalance, Order, Transfer, HoldingAllocation
)
from flask_socketio import SocketIO, emit

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///nav_demo.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# --- Config esterno ---
app.config["EXT_API_BASE"] = os.getenv("EXT_API_BASE", "").rstrip("/")
app.config["EXT_API_KEY"]  = os.getenv("EXT_API_KEY", "")
app.config["EXT_POLL_SECONDS"] = int(os.getenv("EXT_POLL_SECONDS", "1"))
app.config["EXT_POLL_BACKFILL_DAYS"] = int(os.getenv("EXT_POLL_BACKFILL_DAYS", "7"))

CORS(app, supports_credentials=True)
db.init_app(app)
socketio = SocketIO(app, cors_allowed_origins="*")
TZ = pytz.timezone("Europe/Rome")
getcontext().prec = 28

UNIT_EUR = Decimal("0.01")

# ---------------- utils comuni ----------------
def parse_range(args):
    r = args.get("range", "4w")
    if r == "all":
        return None, None
    today = datetime.now(TZ).date()
    if r == "2w":
        end = today; start = end - timedelta(days=13)
    elif r == "4w":
        end = today; start = end - timedelta(days=27)
    elif r == "custom":
        s = args.get("start"); e = args.get("end")
        if not (s and e): return None, None
        start = datetime.fromisoformat(s).date(); end = datetime.fromisoformat(e).date()
    else:
        end = today; start = end - timedelta(days=27)
    return start, end

def prev_range(start: date, end: date):
    delta = (end - start) + timedelta(days=1)
    prev_end = start - timedelta(days=1)
    prev_start = prev_end - delta + timedelta(days=1)
    return prev_start, prev_end

def week_id(dt: datetime) -> str:
    y, w, _ = dt.isocalendar()
    return f"{y}-W{w:02d}"

def week_bounds(d: date):
    start = datetime.combine(d, datetime.min.time()).astimezone(TZ)
    end   = start + timedelta(days=7) - timedelta(microseconds=1)
    return start, end

def weeks_in_range(start_date: date, end_date: date):
    cur = start_date - timedelta(days=start_date.weekday())
    end_monday = end_date - timedelta(days=end_date.weekday())
    while cur <= end_monday:
        yield cur
        cur += timedelta(days=7)

def ensure_month(month_ym: str):
    try:
        datetime.strptime(month_ym + "-01", "%Y-%m-%d")
        return True
    except Exception:
        return False

# ---------------- Helpers Revenue Panel ----------------
def parse_date_any(s: str):
    if not s:
        return None
    try:
        if len(s) == 10:
            return datetime.strptime(s, "%Y-%m-%d").date()
        if len(s) == 7:
            return datetime.strptime(s + "-01", "%Y-%m-%d").date()
    except Exception:
        return None
    return None

def months_in_range(start_date: date, end_date: date):
    y, m = start_date.year, start_date.month
    while (y, m) <= (end_date.year, end_date.month):
        yield date(y, m, 1)
        m += 1
        if m == 13:
            m = 1
            y += 1

def years_in_range(start_date: date, end_date: date):
    for y in range(start_date.year, end_date.year + 1):
        yield y

def dt_start(d: date) -> datetime:
    return TZ.localize(datetime.combine(d, datetime.min.time()))

def dt_end(d: date) -> datetime:
    return TZ.localize(datetime.combine(d, datetime.max.time()))

def to_local_date(dt_obj: datetime) -> date:
    try:
        if dt_obj.tzinfo is None:
            dt_obj = pytz.UTC.localize(dt_obj)
        return dt_obj.astimezone(TZ).date()
    except Exception:
        return dt_obj.date()

def parse_revenue_params(args):
    mode = (args.get("mode") or "month").lower()
    if mode == "all":
        mode = "since"
    if mode not in {"day", "week", "month", "year", "since", "custom"}:
        mode = "month"

    p_raw = (args.get("plans") or "").upper().replace(" ", "")
    plans = {p for p in p_raw.split(",") if p in {"A", "B", "C"}}
    if not plans:
        plans = {"A", "B", "C"}

    today = datetime.now(TZ).date()
    start_q = parse_date_any(args.get("start") or "")
    end_q = parse_date_any(args.get("end") or "")

    if mode == "since":
        first_dt = db.session.query(func.min(Distribution.created_at)).scalar()
        start = to_local_date(first_dt) if first_dt else today
        end = today
        group = "month"
    elif mode == "day":
        start = start_q or (today - timedelta(days=13)); end = end_q or today; group = "day"
    elif mode == "week":
        start = start_q or (today - timedelta(weeks=11)); end = end_q or today; group = "week"
    elif mode == "month":
        if start_q and end_q:
            start, end = start_q, end_q
        else:
            end = today
            y2, m2 = end.year, end.month - 11
            while m2 <= 0: m2 += 12; y2 -= 1
            start = date(y2, m2, 1)
        group = "month"
    elif mode == "year":
        start = start_q or date(today.year - 4, 1, 1); end = end_q or today; group = "year"
    else:
        start = start_q or (today - timedelta(days=89)); end = end_q or today
        span = (end - start).days + 1
        group = "day" if span <= 40 else ("month" if span <= 400 else "year")

    if end < start:
        start, end = end, start
    return mode, group, start, end, plans

def revenue_bucket_keys(group: str, start: date, end: date):
    keys = []
    if group == "day":
        cur = start
        while cur <= end:
            keys.append(cur.isoformat()); cur += timedelta(days=1)
    elif group == "week":
        for monday in weeks_in_range(start, end):
            y, w, _ = datetime.combine(monday, datetime.min.time()).isocalendar()
            keys.append(f"{y}-W{int(w):02d}")
    elif group == "month":
        for d in months_in_range(start, end):
            keys.append(d.strftime("%Y-%m"))
    else:
        for y in years_in_range(start, end):
            keys.append(str(y))
    return keys

def distribution_bucket_key(rec: Distribution, group: str) -> str:
    if group == "month":
        return rec.month_ym
    if group == "year":
        return (rec.month_ym or "")[:4]
    dt_local = to_local_date(rec.created_at)
    if group == "day":
        return dt_local.isoformat()
    y, w, _ = datetime.combine(dt_local, datetime.min.time()).isocalendar()
    return f"{y}-W{int(w):02d}"

def decimal_to_float2(x: Decimal) -> float:
    return float((x or Decimal("0")).quantize(Decimal("0.01")))

# ---------------- Helpers import esterno ----------------
def plan_code_from_name_id(name: str, plan_id: str) -> str:
    n = (name or "").strip().lower()
    pid = (plan_id or "").strip().lower()
    if "smart" in n or "smart_yield" in n or "smart" in pid: return "A"
    if "premium" in n or "premium" in pid: return "B"
    if "platinum" in n or "platinum" in pid: return "C"
    return "A"

def _to_eur_cents_from_amount(amount, currency: Optional[str]) -> int:
    """USDC ≈ EUR (1:1)."""
    try:
        d = Decimal(str(amount or 0))
    except Exception:
        d = Decimal("0")
    return int((d * Decimal("100")).to_integral_value(rounding=ROUND_HALF_UP))

def user_plan_total_eur_cents(plan_obj: dict) -> int:
    """Usa depositedAmount; fallback totalAmount."""
    if plan_obj is None:
        return 0
    if plan_obj.get("depositedAmount") is not None:
        return _to_eur_cents_from_amount(plan_obj.get("depositedAmount"), plan_obj.get("currency"))
    return _to_eur_cents_from_amount(plan_obj.get("totalAmount"), plan_obj.get("currency"))

def tx_amount_eur_cents(tx: dict) -> int:
    return _to_eur_cents_from_amount(tx.get("amount"), tx.get("currency"))

def epoch_to_date_utc(val) -> date:
    if val is None:
        return datetime.utcnow().date()
    try:
        val = int(val)
        ts = val / 1000.0 if val > 10**11 else float(val)
        return datetime.utcfromtimestamp(ts).date()
    except Exception:
        return datetime.utcnow().date()

def ext_session():
    s = requests.Session()
    if app.config["EXT_API_KEY"]:
        s.headers.update({"X-API-Key": app.config["EXT_API_KEY"]})
    s.headers.update({"Accept": "application/json"})
    return s

def ext_get(path: str, params: dict) -> dict:
    base = app.config["EXT_API_BASE"]
    if not base:
        return {}
    url = f"{base}{path}"
    r = ext_session().get(url, params=params, timeout=10)
    r.raise_for_status()
    return r.json()

# ---------------- Modelli ausiliari ----------------
class ExternalPlanMap(db.Model):
    """
    Mappa (userId, planId) esterni -> Registration.id interno
    + plan_name / currency per popolare la tabella Users/Distribution.
    """
    __tablename__ = "external_plan_map"
    external_user_id = db.Column(db.String(128), primary_key=True)
    external_plan_id = db.Column(db.String(128), primary_key=True)
    registration_id  = db.Column(db.Integer, db.ForeignKey("registrations.id"), nullable=False, index=True)
    plan_code        = db.Column(db.String(1), nullable=False)   # 'A'|'B'|'C'
    plan_name        = db.Column(db.String(128), nullable=True)  # es. "Smart yield"
    currency         = db.Column(db.String(16), nullable=True)   # es. "USDC"

class ExternalTxMap(db.Model):
    __tablename__ = "external_tx_map"
    transaction_id = db.Column(db.String(128), primary_key=True)
    order_id = db.Column(db.Integer, db.ForeignKey("orders.id"), nullable=True, index=True)

class SyncState(db.Model):
    __tablename__ = "sync_state"
    key = db.Column(db.String(64), primary_key=True)
    value = db.Column(db.String(255), nullable=False)

# mini-migrazione SQLite per le nuove colonne
def _ensure_epm_columns():
    insp = inspect(db.engine)
    try:
        cols = {c['name'] for c in insp.get_columns('external_plan_map')}
    except Exception:
        return
    with db.engine.begin() as conn:
        if 'plan_name' not in cols:
            conn.execute(sqltext("ALTER TABLE external_plan_map ADD COLUMN plan_name VARCHAR(128)"))
        if 'currency' not in cols:
            conn.execute(sqltext("ALTER TABLE external_plan_map ADD COLUMN currency VARCHAR(16)"))

def _get_state(key: str, default: Optional[str] = None) -> Optional[str]:
    row = SyncState.query.get(key)
    return row.value if row else default

def _set_state(key: str, value: str) -> None:
    row = SyncState.query.get(key)
    if row: row.value = value
    else:   db.session.add(SyncState(key=key, value=value))
    db.session.commit()

# ---------------- Upsert dal vendor ----------------
def upsert_user(item: dict):
    """
    item vendor:
    {
      "userId":"...", "fullName":"...",
      "connectedPlans":[
        {"id":"...", "name":"Smart yield", "firstDepositDate":..., "depositedAmount":..., "currency":"USDC", "totalAmount":...}
      ]
    }
    """
    ext_user = str(item.get("userId") or "")
    full_name = item.get("fullName") or ""
    plans = item.get("connectedPlans") or []
    for p in plans:
        plan_id   = str(p.get("id") or "")
        plan_name = p.get("name") or ""
        plan_code = plan_code_from_name_id(plan_name, plan_id)
        currency  = (p.get("currency") or "").upper() or None

        # Salviamo come capitale il depositedAmount (fallback totalAmount)
        total_eur_cents = user_plan_total_eur_cents(p)
        deposited_eur = (Decimal(total_eur_cents) / Decimal(100)).quantize(Decimal("0.01"))

        fd = p.get("firstDepositDate")
        reg_date = epoch_to_date_utc(fd)

        mapping = ExternalPlanMap.query.get((ext_user, plan_id))
        if mapping:
            reg = Registration.query.get(mapping.registration_id)
            if reg:
                reg.full_name = full_name
                reg.plan = plan_code
                reg.amount = deposited_eur
            mapping.plan_code = plan_code
            mapping.plan_name = plan_name or mapping.plan_name
            mapping.currency  = currency or mapping.currency
        else:
            reg = Registration(
                full_name = full_name,
                plan      = plan_code,
                amount    = deposited_eur,
                created_at= TZ.localize(datetime.combine(reg_date, datetime.min.time()))
            )
            db.session.add(reg); db.session.flush()
            db.session.add(ExternalPlanMap(
                external_user_id = ext_user,
                external_plan_id = plan_id,
                registration_id  = reg.id,
                plan_code        = plan_code,
                plan_name        = plan_name,
                currency         = currency
            ))

def upsert_transaction(item: dict, plans_to_net: set):
    """
    item vendor:
    {
      "id":"tx...", "userId":"...", "planId":"...", "name":"Smart yield",
      "currency":"USDC", "date":..., "amount": 100 (signed), "status":"PENDING"/"ACTIVE"/...
    }
    """
    tx_id = str(item.get("id") or item.get("transaction_id") or "")
    if not tx_id: return
    if ExternalTxMap.query.get(tx_id): return

    ext_user = str(item.get("userId") or "")
    plan_id  = str(item.get("planId") or "")
    plan_name= item.get("name") or ""
    plan_code= plan_code_from_name_id(plan_name, plan_id)

    amt_eur_cents = tx_amount_eur_cents(item)  # signed
    side = "IN" if amt_eur_cents > 0 else "OUT"
    amount_eur = (Decimal(abs(amt_eur_cents)) / Decimal(100)).quantize(Decimal("0.01"))

    eff_date = epoch_to_date_utc(item.get("date"))

    mapping = ExternalPlanMap.query.get((ext_user, plan_id))
    if not mapping:
        return
    reg_id = mapping.registration_id

    o = Order(
        registration_id=reg_id,
        plan           = plan_code,
        side           = side,
        amount_eur     = amount_eur,
        units          = int((amount_eur / UNIT_EUR).to_integral_value(rounding=ROUND_FLOOR)),
        status         = "PENDING" if (str(item.get("status","")).upper() in ("PENDING","",None)) else "PENDING",
        created_at     = TZ.localize(datetime.combine(eff_date, datetime.min.time()))
    )
    db.session.add(o); db.session.flush()
    db.session.add(ExternalTxMap(transaction_id=tx_id, order_id=o.id))
    plans_to_net.add(plan_code)

# ---------------- Polling ----------------
def _as_items(resp_json):
    if isinstance(resp_json, list):
        return resp_json
    return resp_json.get("items", []) or []

def sync_once_users():
    last = _get_state("users_updated_since", None)
    params = {"page_size": 500}
    if last: params["updated_since"] = last
    while True:
        data = ext_get("/api/v1/export/users", params)
        items = _as_items(data)
        if not items: break
        for it in items:
            upsert_user(it)
        db.session.commit()
        now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        _set_state("users_updated_since", now_iso)
        cursor = data.get("next_cursor") if isinstance(data, dict) else None
        if cursor: params["cursor"] = cursor
        else: break

def sync_once_transactions():
    start = _get_state("tx_start", None)
    if not start:
        start_dt = datetime.utcnow() - timedelta(days=app.config["EXT_POLL_BACKFILL_DAYS"])
        start = start_dt.strftime("%Y-%m-%d")
    end = datetime.utcnow().strftime("%Y-%m-%d")

    params = {"start": start, "end": end, "page_size": 500}
    plans_to_net = set()
    while True:
        data = ext_get("/api/v1/export/transactions", params)
        items = _as_items(data)
        if not items: break
        for it in items:
            upsert_transaction(it, plans_to_net)
        db.session.commit()
        cursor = data.get("next_cursor") if isinstance(data, dict) else None
        if cursor: params["cursor"] = cursor
        else: break

    for p in plans_to_net:
        try:
            summary = _run_netting_for_plan(p)
            socketio.emit("netting-updated", {"plan": p, "summary": summary})
        except Exception:
            pass

    new_start = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    _set_state("tx_start", new_start)

def poll_loop():
    if not app.config["EXT_API_BASE"] or not app.config["EXT_API_KEY"]:
        return
    while True:
        try:
            with app.app_context():
                sync_once_users()
                sync_once_transactions()
                socketio.emit("data-updated", {})
        except Exception as e:
            print("[poll_loop] error:", e)
        time.sleep(app.config["EXT_POLL_SECONDS"])

# ---------------- pages ----------------
@app.route("/")
def dashboard():
    return render_template("dashboard.html")

# ---------------- AuA (pie) ----------------
@app.route("/api/summary")
def api_summary():
    start, end = parse_range(request.args)
    if start is None and end is None:
        rows = db.session.query(Registration.plan, func.coalesce(func.sum(Registration.amount), 0))\
            .group_by(Registration.plan).all()
        range_info = {"mode": "all", "start": None, "end": None}
    else:
        start_dt = datetime.combine(start, datetime.min.time()).astimezone(TZ)
        end_dt   = datetime.combine(end, datetime.max.time()).astimezone(TZ)
        rows = db.session.query(Registration.plan, func.coalesce(func.sum(Registration.amount), 0))\
            .filter(Registration.created_at >= start_dt, Registration.created_at <= end_dt)\
            .group_by(Registration.plan).all()
        range_info = {"mode": "range", "start": start.isoformat(), "end": end.isoformat()}

    by_plan = {p: float(v or 0) for p, v in rows}
    for p in ("A","B","C"): by_plan.setdefault(p, 0.0)
    total = sum(by_plan.values())
    return jsonify({"range": range_info, "capital_by_plan": by_plan, "capital_total": total})

# -------- NEW: users_counts per AUA Pie (conteggio utenti per piano) --------
@app.route("/api/users_counts", methods=["GET"])
def api_users_counts():
    """
    Conta gli utenti (registrations) per piano nel range selezionato.
    Query: ?start=YYYY-MM-DD&end=YYYY-MM-DD (entrambi opzionali)
    """
    start_q = parse_date_any(request.args.get("start") or "")
    end_q   = parse_date_any(request.args.get("end") or "")

    q = db.session.query(Registration.plan, func.count(Registration.id))
    if start_q or end_q:
        start_dt = dt_start(start_q or date(1900,1,1))
        end_dt   = dt_end(end_q or date(2100,12,31))
        q = q.filter(Registration.created_at >= start_dt, Registration.created_at <= end_dt)

    rows = q.group_by(Registration.plan).all()
    counts = {p: int(c or 0) for p, c in rows}
    for p in ("A","B","C"): counts.setdefault(p, 0)

    return jsonify({
        "counts_by_plan": counts,
        "start": start_q.isoformat() if start_q else None,
        "end": end_q.isoformat() if end_q else None
    })

# ---------------- Subscriptions & Redemptions (weekly) ----------------
@app.route("/api/stats")
def api_stats():
    start, end = parse_range(request.args)
    if start is None and end is None:
        end = datetime.now(TZ).date()
        start = end - timedelta(days=83)

    start_dt_abs = datetime.combine(start, datetime.min.time()).astimezone(TZ)
    end_dt_abs   = datetime.combine(end, datetime.max.time()).astimezone(TZ)

    users_rows = Registration.query\
        .filter(Registration.created_at >= start_dt_abs, Registration.created_at <= end_dt_abs).all()
    latest_users = [r.to_dict() for r in users_rows]
    latest_users.sort(key=lambda x: x["created_at"], reverse=True)
    totals_by_plan = {"A":0,"B":0,"C":0}
    for r in users_rows: totals_by_plan[r.plan] = totals_by_plan.get(r.plan, 0) + 1

    weekly = []
    now_tz = datetime.now(TZ)
    for monday in weeks_in_range(start, end):
        ws_dt, we_dt = week_bounds(monday)
        cur_end = min(we_dt, now_tz)
        bucket = {"week_start": monday.isoformat()}
        for plan in ("A","B","C"):
            committed = db.session.query(func.coalesce(func.sum(Order.amount_eur), 0))\
                .filter(Order.plan==plan, Order.side=="IN",
                        Order.created_at>=ws_dt, Order.created_at<=cur_end).scalar() or 0
            recalled = db.session.query(func.coalesce(func.sum(Order.amount_eur), 0))\
                .filter(Order.plan==plan, Order.side=="OUT", Order.status=="PENDING",
                        Order.created_at>=ws_dt, Order.created_at<=cur_end).scalar() or 0
            committed_f = float(committed); recalled_f = float(recalled)
            bucket[plan] = {"committed": committed_f, "recalled": recalled_f, "net": round(committed_f-recalled_f, 2)}
        weekly.append(bucket)

    return jsonify({
        "range": {"start": start.isoformat(), "end": end.isoformat()},
        "weekly_flows": weekly,
        "latest_users": latest_users[:500],
        "totals_by_plan": totals_by_plan
    })

# ---------------- Diversification ----------------
@app.route("/api/diversification", methods=["GET"])
def api_diversification():
    dim = (request.args.get("dimension") or "plan").lower()
    if dim not in ("plan","asset_class","strategy","gp"):
        return jsonify({"error":"dimension must be one of plan|asset_class|strategy|gp"}), 400

    col = {
        "plan": HoldingAllocation.plan,
        "asset_class": HoldingAllocation.asset_class,
        "strategy": HoldingAllocation.strategy,
        "gp": HoldingAllocation.gp
    }[dim]

    rows = db.session.query(col.label("key"), func.coalesce(func.sum(HoldingAllocation.nav_eur), 0))\
        .group_by(col).order_by(col.asc()).all()
    data = [{"key": k, "value": float(v or 0)} for k, v in rows]
    return jsonify({"dimension": dim, "items": data})

@app.route("/api/diversification", methods=["POST"])
def api_diversification_upsert():
    body = request.get_json(force=True) or {}
    items = body.get("items") or []
    if not isinstance(items, list) or not items:
        return jsonify({"error":"items must be a non-empty list"}), 400

    for it in items:
        plan = (it.get("plan") or "").upper()
        if plan not in ("A","B","C"):
            return jsonify({"error":f"invalid plan {plan}"}), 400
        asset_class = it.get("asset_class") or "Fund"
        strategy = it.get("strategy") or "Unspecified"
        gp = it.get("gp") or "Unspecified"
        nav = Decimal(str(it.get("nav_eur", 0))).quantize(Decimal("0.01"))

        rec = HoldingAllocation.query.filter_by(plan=plan, asset_class=asset_class, strategy=strategy, gp=gp).first()
        if rec:
            rec.nav_eur = nav; rec.updated_at = datetime.utcnow()
        else:
            db.session.add(HoldingAllocation(plan=plan, asset_class=asset_class, strategy=strategy, gp=gp, nav_eur=nav))

    db.session.commit()
    socketio.emit("diversification-updated", {})
    return jsonify({"ok": True}), 200

# ---------------- METRICS ----------------
@app.route("/api/metrics")
def api_metrics():
    aum_rows = db.session.query(Registration.plan, func.coalesce(func.sum(Registration.amount), 0))\
        .group_by(Registration.plan).all()
    aum_by_plan = {p: float(v or 0) for p,v in aum_rows}
    for p in ("A","B","C"): aum_by_plan.setdefault(p, 0.0)
    aum_total = sum(aum_by_plan.values())

    nav_rows = db.session.query(HoldingAllocation.plan, func.coalesce(func.sum(HoldingAllocation.nav_eur), 0))\
        .group_by(HoldingAllocation.plan).all()
    nav_by_plan = {p: float(v or 0) for p,v in nav_rows}
    for p in ("A","B","C"): nav_by_plan.setdefault(p, 0.0)
    deployment_rate = {}
    for p in ("A","B","C"):
        denom = aum_by_plan[p] or 0.0
        deployment_rate[p] = (nav_by_plan[p]/denom*100.0) if denom>0 else 0.0

    cash_row = db.session.query(func.coalesce(func.sum(HoldingAllocation.nav_eur), 0))\
        .filter(HoldingAllocation.asset_class=="Cash").scalar() or 0
    cash_on_hand = float(cash_row)

    start_r, end_r = parse_range(request.args)
    if start_r is None or end_r is None:
        end_r = datetime.now(TZ).date()
        start_r = end_r - timedelta(days=27)
    prev_s, prev_e = prev_range(start_r, end_r)

    def aum_in_period(s: date, e: date):
        sdt = datetime.combine(s, datetime.min.time()).astimezone(TZ)
        edt = datetime.combine(e, datetime.max.time()).astimezone(TZ)
        rows = db.session.query(Registration.plan, func.coalesce(func.sum(Registration.amount), 0))\
            .filter(Registration.created_at >= sdt, Registration.created_at <= edt)\
            .group_by(Registration.plan).all()
        d = {p: float(v or 0) for p,v in rows}
        for p in ("A","B","C"): d.setdefault(p, 0.0)
        return d, sum(d.values())

    cur_by_plan, cur_total = aum_in_period(start_r, end_r)
    prev_by_plan, prev_total = aum_in_period(prev_s, prev_e)

    def growth(cur, prev):
        return ((cur - prev) / prev * 100.0) if prev else (100.0 if cur>0 and prev==0 else 0.0)

    growth_total = growth(cur_total, prev_total)
    growth_by_plan = {p: growth(cur_by_plan[p], prev_by_plan.get(p,0.0)) for p in ("A","B","C")}

    sdt = datetime.combine(start_r, datetime.min.time()).astimezone(TZ)
    edt = datetime.combine(end_r, datetime.max.time()).astimezone(TZ)
    total_in = float(db.session.query(func.coalesce(func.sum(Order.amount_eur), 0))
                     .filter(Order.side=="IN", Order.created_at>=sdt, Order.created_at<=edt).scalar() or 0)
    total_out_pending = float(db.session.query(func.coalesce(func.sum(Order.amount_eur), 0))
                              .filter(Order.side=="OUT", Order.status=="PENDING",
                                      Order.created_at>=sdt, Order.created_at<=edt).scalar() or 0)
    denom = total_in + total_out_pending
    redemption_rate = (total_out_pending/denom*100.0) if denom>0 else 0.0

    months = db.session.query(MonthlyReturn.month_ym).distinct().all()
    months = [m[0] for m in months]
    irr_gross = {p:0.0 for p in ("A","B","C")}
    irr_net   = {p:0.0 for p in ("A","B","C")}
    if months:
        for p in ("A","B","C"):
            rows = db.session.query(MonthlyReturn.percent).filter(MonthlyReturn.plan==p).all()
            vals = [float(r[0] or 0) for r in rows]
            if vals:
                gross = sum(vals)/len(vals)
                irr_gross[p] = gross
                irr_net[p] = gross - 2.0

    signup_count = db.session.query(func.count(Registration.id)).scalar() or 0
    kyc_count = 0
    funded_count = 0
    committed_count = int(signup_count > 0)
    def rate(a,b): return (a/b*100.0) if b else 0.0

    return jsonify({
        "range": {"start": start_r.isoformat(), "end": end_r.isoformat()},
        "strategies_portfolio": {
            "aum": {
                "total": aum_total,
                "by_plan": aum_by_plan,
                "growth_total_pct": growth_total,
                "growth_by_plan_pct": growth_by_plan
            },
            "irr": { "gross_pct_by_plan": irr_gross, "net_pct_by_plan": irr_net },
            "deployment_rate_pct_by_plan": deployment_rate,
            "liquidity": { "redemption_rate_pct": redemption_rate, "cash_on_hand": cash_on_hand }
        },
        "user_engagement": {
            "signup": {"count": signup_count},
            "kyc": {"count": kyc_count, "conv_from_signup_pct": rate(kyc_count, signup_count)},
            "funded": {"count": funded_count, "conv_from_kyc_pct": rate(funded_count, kyc_count)},
            "committed": {"count": committed_count, "conv_from_funded_pct": rate(committed_count, funded_count)}
        }
    })

# ---------------- Returns & Distribution ----------------
@app.route("/api/returns", methods=["GET"])
def api_get_returns():
    month = request.args.get("month")
    if not month or not ensure_month(month):
        return jsonify({"error":"month (YYYY-MM) is required"}), 400
    rows = MonthlyReturn.query.filter_by(month_ym=month).all()
    by_plan = {r.plan: float(r.percent or 0) for r in rows}
    for p in ("A","B","C"): by_plan.setdefault(p, 0.0)
    res = MonthlyResult.query.filter_by(month_ym=month).first()
    total_eur = float(res.total_eur) if res else 0.0
    return jsonify({"month": month, "returns": by_plan, "total_eur": total_eur})

@app.route("/api/returns", methods=["POST"])
def api_set_returns():
    data = request.get_json(force=True)
    month = data.get("month")
    if not month or not ensure_month(month):
        return jsonify({"error":"month (YYYY-MM) is required"}), 400
    for plan in ("A","B","C"):
        value = Decimal(str(data.get(plan, 0))).quantize(Decimal("0.01"))
        rec = MonthlyReturn.query.filter_by(month_ym=month, plan=plan).first()
        if rec: rec.percent = value
        else:   db.session.add(MonthlyReturn(month_ym=month, plan=plan, percent=value))
    total_eur = Decimal(str(data.get("total_eur", 0))).quantize(Decimal("0.01"))
    res = MonthlyResult.query.filter_by(month_ym=month).first()
    if res: res.total_eur = total_eur
    else:   db.session.add(MonthlyResult(month_ym=month, total_eur=total_eur))
    db.session.commit()
    socketio.emit("returns-updated", {"month": month})
    return jsonify({"ok": True, "month": month}), 200

# ---------- USERS table ----------
def _plan_order_key(p: str) -> int:
    return {"A":0, "B":1, "C":2}.get(p, 9)

def _build_user_groups(plan_filter: Optional[str], search_q: str, sort: str,
                       start_date: Optional[date] = None, end_date: Optional[date] = None):
    """
    Blocchi utente -> righe per tab Users.
    Campi:
      code=userId, full_name=fullName, plan_id=id, plan=name,
      deposited_amount=depositedAmount, currency, date=firstDepositDate (ISO)
    Filtri opzionali su start_date/end_date (prima data deposito = Registration.created_at).
    """
    EPM = ExternalPlanMap
    recs = (db.session.query(
                Registration,
                EPM.external_user_id,
                EPM.external_plan_id,
                EPM.plan_name,
                EPM.currency
            )
            .outerjoin(EPM, EPM.registration_id == Registration.id)
            .all())

    items: List[Dict[str, Any]] = []
    for reg, ext_uid, ext_pid, plan_name, currency in recs:
        if plan_filter in {"A","B","C"} and reg.plan != plan_filter:
            continue
        created_d = to_local_date(reg.created_at) if reg.created_at else None
        if (start_date or end_date) and created_d:
            if start_date and created_d < start_date: 
                continue
            if end_date and created_d > end_date: 
                continue
        if search_q and search_q not in (reg.full_name or "").lower():
            continue
        # assicuriamo sempre un nome piano "parlante" per i regex del frontend
        plan_label = plan_name or {"A":"Smart Yield","B":"Premium","C":"Platinum"}.get(reg.plan, reg.plan)
        items.append({
            "code": ext_uid or f"REG-{reg.id:06d}",
            "full_name": reg.full_name or "",
            "plan_id": ext_pid or "",
            "plan": plan_label,
            "plan_code": reg.plan,
            "deposited_amount": float(reg.amount or 0),
            "currency": (currency or "USDC"),
            "date": created_d.isoformat() if created_d else ""
        })

    # Raggruppa per utente (code)
    from collections import defaultdict
    groups = defaultdict(list)
    names = {}
    for it in items:
        groups[it["code"]].append(it)
        names[it["code"]] = it["full_name"]

    # ordina righe del blocco per plan code A/B/C
    for k in groups:
        groups[k].sort(key=lambda r: _plan_order_key(r["plan_code"]))

    # ordina gruppi
    keys = list(groups.keys())
    if sort == "name":
        keys.sort(key=lambda k: names[k].lower())
    elif sort == "oldest":
        def k_oldest(k):
            dates = [g["date"] for g in groups[k] if g["date"]]
            mn = min(dates) if dates else "9999-12-31"
            return (mn, names[k].lower())
        keys.sort(key=k_oldest)
    else:  # newest
        def k_newest(k):
            dates = [g["date"] for g in groups[k] if g["date"]]
            mx = max(dates) if dates else ""
            return (mx, names[k].lower())
        keys.sort(key=k_newest, reverse=True)

    # counts per badge (per utente unico)
    count_A = sum(1 for k in keys if any(r["plan_code"]=="A" for r in groups[k]))
    count_B = sum(1 for k in keys if any(r["plan_code"]=="B" for r in groups[k]))
    count_C = sum(1 for k in keys if any(r["plan_code"]=="C" for r in groups[k]))

    blocks = [{
        "code": k,
        "full_name": names[k],
        "rows": groups[k]
    } for k in keys]

    return blocks, {"A":count_A, "B":count_B, "C":count_C}, len(blocks)

def _paginate_blocks(blocks: List[Dict[str, Any]], page: int, per_rows: int):
    pages: List[List[Dict[str, Any]]] = []
    cur: List[Dict[str, Any]] = []
    used = 0
    for b in blocks:
        size = len(b["rows"])
        if used + size > per_rows and cur:
            pages.append(cur); cur=[]; used=0
        cur.append(b); used += size
    if cur: pages.append(cur)
    total_pages = max(1, len(pages))
    page = max(1, min(page, total_pages))
    return pages[page-1] if pages else [], total_pages

@app.route("/api/users_table", methods=["GET"])
def api_users_table():
    q = (request.args.get("q") or "").strip().lower()
    plan_filter = request.args.get("plan")
    sort = (request.args.get("sort") or "newest").lower()   # newest | oldest | name
    rows_per_page = int(request.args.get("rows", 10))
    page = int(request.args.get("page", 1))

    # NEW: filtri opzionali per il fallback del grafico AUA
    start_f = parse_date_any(request.args.get("start") or "")
    end_f   = parse_date_any(request.args.get("end") or "")

    blocks, plan_counts, total_users = _build_user_groups(plan_filter, q, sort, start_f, end_f)
    page_blocks, total_pages = _paginate_blocks(blocks, page, rows_per_page)

    rows_out: List[Dict[str, Any]] = []
    for b in page_blocks:
        first = True
        for r in b["rows"]:
            rows_out.append({
                "code": b["code"] if first else "",
                "full_name": b["full_name"] if first else "",
                "plan_id": r["plan_id"],
                "plan": r["plan"],
                "deposited_amount": r["deposited_amount"],
                "currency": r["currency"],
                "date": r["date"],
            })
            first = False

    return jsonify({
        "q": q,
        "plan_filter": plan_filter,
        "sort": sort,
        "rows_per_page": rows_per_page,
        "page": page,
        "total_pages": total_pages,
        "total_users": total_users,
        "counts_by_plan": plan_counts,
        "rows": rows_out
    })

@app.route("/api/users_table/export.csv")
def api_users_table_export():
    blocks, _, _ = _build_user_groups(plan_filter=None, search_q="", sort="name")
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Code","Full Name","Plan ID","Plan","Deposited Amount (EUR)","Currency","Date"])
    for b in blocks:
        code = b["code"]; full = b["full_name"]
        for r in b["rows"]:
            w.writerow([code, full, r["plan_id"], r["plan"], f"{r['deposited_amount']:.2f}", r["currency"], r["date"]])
    return Response(buf.getvalue().encode("utf-8"),
                    mimetype="text/csv",
                    headers={"Content-Disposition": "attachment; filename=users.csv"})

# ---------- DISTRIBUTION ----------
def _build_distribution_table_rows(month: str) -> List[Dict[str, Any]]:
    """
    Ritorna righe per tabella Distribution con i calcoli richiesti.
    """
    EPM = ExternalPlanMap
    q = (db.session.query(Distribution, Registration, EPM.external_user_id, EPM.external_plan_id, EPM.currency, EPM.plan_name)
         .join(Registration, Registration.id == Distribution.registration_id)
         .outerjoin(EPM, EPM.registration_id == Registration.id)
         .filter(Distribution.month_ym == month)
         .order_by(Distribution.full_name.asc(), Distribution.plan.asc()))
    records = q.all()
    if not records:
        return []

    from collections import defaultdict
    grouped = defaultdict(list)
    names: Dict[str,str] = {}
    for dist, reg, ext_uid, ext_pid, curr, plan_name in records:
        code = ext_uid or f"REG-{reg.id:06d}"
        names[code] = dist.full_name or reg.full_name or ""

        invested = Decimal(str(dist.invested_amount or 0))
        pct      = Decimal(str(dist.effective_percent or 0))
        gross    = Decimal(str(dist.payout_amount or 0))                  # Gross Profit
        kleos    = (gross * Decimal("0.20")).quantize(Decimal("0.01"))    # 20% del Gross
        net      = (gross - kleos).quantize(Decimal("0.01"))
        dep_plus = (invested + net).quantize(Decimal("0.01"))

        # FIX: restituiamo sempre nomi piani completi per i regex JS
        plan_label = plan_name or {"A":"Smart Yield","B":"Premium","C":"Platinum"}.get(reg.plan, reg.plan)

        grouped[code].append({
            "full_name": names[code],
            "plan_id": ext_pid or "",
            "plan": plan_label,
            "deposited_amount": decimal_to_float2(invested),
            "currency": (curr or "USDC"),
            "pct_profit": float(pct.quantize(Decimal("0.000001"))),
            "dep_plus_profit": decimal_to_float2(dep_plus),
            "gross_profit": decimal_to_float2(gross),
            "kleos_revenue": decimal_to_float2(kleos),
            "net_profit": decimal_to_float2(net),
            "plan_code": reg.plan
        })

    out: List[Dict[str, Any]] = []
    for code in sorted(grouped.keys(), key=lambda k: names[k].lower()):
        rows = sorted(grouped[code], key=lambda r: _plan_order_key(r["plan_code"]))
        first = True
        for r in rows:
            out.append({
                "code": code if first else "",
                "full_name": r["full_name"] if first else "",
                "plan_id": r["plan_id"],
                "plan": r["plan"],
                "deposited_amount": r["deposited_amount"],
                "currency": r["currency"],
                "pct_profit": r["pct_profit"],
                "dep_plus_profit": r["dep_plus_profit"],
                "gross_profit": r["gross_profit"],
                "kleos_revenue": r["kleos_revenue"],
                "net_profit": r["net_profit"],
            })
            first = False
    return out

@app.route("/api/distributions", methods=["GET"])
def api_get_distributions():
    month = request.args.get("month")
    view  = (request.args.get("view") or "").lower()
    if not month or not ensure_month(month):
        return jsonify({"error":"month (YYYY-MM) is required"}), 400

    items_raw = [r.to_dict() for r in Distribution.query
                 .filter_by(month_ym=month)
                 .order_by(Distribution.full_name.asc()).all()]

    table_rows = _build_distribution_table_rows(month)

    if view in {"table", "expanded"}:
        return jsonify({"month": month, "rows": table_rows})

    return jsonify({"month": month, "items": items_raw, "table_rows": table_rows})

@app.route("/api/distribute", methods=["POST"])
def api_run_distribution():
    """
    Nuova logica:
      invested = Registration.amount (Deposited Amount)
      pct      = MonthlyReturn.percent (per piano A/B/C) del mese
      gross    = invested * pct/100
      kleos    = gross * 0.20
      net      = gross - kleos
    Salviamo in Distribution:
      invested_amount = invested
      effective_percent = pct
      payout_amount = gross   (Gross Profit)
    Le colonne calcolate (Net, Deposited+Profit, Kleos) sono derivate nella vista table.
    """
    data = request.get_json(force=True)
    month = data.get("month")
    if not month or not ensure_month(month):
        return jsonify({"error":"month (YYYY-MM) is required"}), 400

    ret_rows = MonthlyReturn.query.filter_by(month_ym=month).all()
    plan_pct = {r.plan: Decimal(str(r.percent or 0)) for r in ret_rows}
    for p in ("A","B","C"): plan_pct.setdefault(p, Decimal("0"))

    Distribution.query.filter_by(month_ym=month).delete()
    db.session.commit()

    created = 0
    for r in Registration.query.all():
        invested = Decimal(str(r.amount or 0))
        pct = plan_pct.get(r.plan, Decimal("0"))
        gross = (invested * pct / Decimal("100")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

        db.session.add(Distribution(
            month_ym=month,
            registration_id=r.id,
            full_name=r.full_name,
            plan=r.plan,
            invested_amount=invested,
            plan_share_percent=pct,
            effective_percent=pct,
            payout_amount=gross
        ))
        created += 1

    db.session.commit()
    socketio.emit("distribution-updated", {"month": month, "count": created})
    return jsonify({"ok": True, "month": month, "count": created}), 200

# ---------- Netting ----------
def _run_netting_for_plan(plan: str):
    batch = week_id(datetime.utcnow())
    buyers = Order.query.filter_by(status="PENDING", side="IN", plan=plan).order_by(Order.created_at.asc()).all()
    sellers = Order.query.filter_by(status="PENDING", side="OUT", plan=plan).order_by(Order.created_at.asc()).all()
    i = j = 0; moved = 0
    while i < len(sellers) and j < len(buyers):
        s = sellers[i]; b = buyers[j]
        take = min(s.units, b.units)
        if take <= 0:
            if s.units <= 0: s.status="FILLED"; s.batch_id=batch; i+=1
            if b.units <= 0: b.status="FILLED"; b.batch_id=batch; j+=1
            continue
        ub_s = UnitBalance.query.filter_by(registration_id=s.registration_id).with_for_update().first()
        ub_b = UnitBalance.query.filter_by(registration_id=b.registration_id).with_for_update().first()
        if not ub_s or ub_s.units <= 0:
            s.status = "CANCELED"; i += 1; continue
        if take > ub_s.units: take = ub_s.units
        if not ub_b:
            ub_b = UnitBalance(registration_id=b.registration_id, plan=plan, units=0)
            db.session.add(ub_b)
        ub_s.units -= take; ub_b.units += take
        s.units -= take; b.units -= take
        moved += take
        db.session.add(Transfer(batch_id=batch, plan=plan,
                                from_registration_id=s.registration_id,
                                to_registration_id=b.registration_id,
                                units=take))
        if s.units == 0: s.status="FILLED"; s.batch_id=batch; i+=1
        else: s.status="PARTIAL"
        if b.units == 0: b.status="FILLED"; b.batch_id=batch; j+=1
        else: b.status="PARTIAL"
    for k in range(i, len(sellers)):
        if sellers[k].units == 0: sellers[k].status="FILLED"; sellers[k].batch_id=batch
    for k in range(j, len(buyers)):
        if buyers[k].units == 0: buyers[k].status="FILLED"; buyers[k].batch_id=batch
    db.session.commit()
    return {"batch": batch, "moved_units": moved, "moved_eur": float(Decimal(moved) * UNIT_EUR)}

@app.route("/api/orders", methods=["POST"])
def api_create_order():
    data = request.get_json(force=True)
    reg_id = int(data.get("registration_id", 0))
    side = (data.get("side") or "").upper()
    amount = Decimal(str(data.get("amount_eur", 0))).quantize(Decimal("0.01"))
    reg = Registration.query.get(reg_id)
    if not reg: return jsonify({"error":"registration_id not found"}), 400
    if side not in ("IN","OUT"): return jsonify({"error":"side must be IN or OUT"}), 400
    if amount <= 0: return jsonify({"error":"amount_eur > 0"}), 400
    units = int((amount / UNIT_EUR).to_integral_value(rounding=ROUND_FLOOR))
    if units <= 0: return jsonify({"error":"amount too small"}), 400
    ub = UnitBalance.query.filter_by(registration_id=reg.id).first()
    if side == "OUT":
        if not ub or ub.units <= 0: return jsonify({"error":"no available units"}), 400
        if units > ub.units: units = ub.units
    o = Order(registration_id=reg.id, plan=reg.plan, side=side, amount_eur=amount, units=units, status="PENDING")
    db.session.add(o); db.session.commit()
    summary = _run_netting_for_plan(reg.plan)
    socketio.emit("netting-updated", {"plan": reg.plan, "summary": summary})
    return jsonify(o.to_dict() | {"netting_summary": summary}), 201

@app.route("/api/orders", methods=["GET"])
def api_list_orders():
    status = request.args.get("status", "PENDING")
    rows = Order.query.filter_by(status=status).order_by(Order.created_at.asc()).all()
    return jsonify({"items": [o.to_dict() for o in rows]})

@app.route("/api/netting/summary", methods=["GET"])
def api_netting_summary():
    batch = request.args.get("batch") or week_id(datetime.utcnow())
    transfers = Transfer.query.filter_by(batch_id=batch).all()
    return jsonify({"batch": batch, "transfers": [t.to_dict() for t in transfers]})

# ---------------- Revenue Panel ----------------
@app.route("/api/revenue_panel", methods=["GET"])
def api_revenue_panel():
    mode, group, start, end, plans = parse_revenue_params(request.args)
    start_dt, end_dt = dt_start(start), dt_end(end)

    keys = revenue_bucket_keys(group, start, end)
    inv_map = {k: Decimal("0") for k in keys}
    card_map = {k: Decimal("0") for k in keys}  # placeholder

    q = Distribution.query.filter(Distribution.created_at >= start_dt,
                                  Distribution.created_at <= end_dt)
    if plans != {"A", "B", "C"}:
        q = q.filter(Distribution.plan.in_(list(plans)))

    for rec in q.all():
        k = distribution_bucket_key(rec, group)
        if k in inv_map:
            pay = Decimal(str(rec.payout_amount or 0))
            kleos = (pay * Decimal("0.20"))
            inv_map[k] += kleos

    investments_series = [{"key": k, "value": decimal_to_float2(v)} for k, v in inv_map.items()]
    investments_total = decimal_to_float2(sum(inv_map.values()))

    cards_series = [{"key": k, "value": decimal_to_float2(v)} for k, v in card_map.items()]
    cards_total = decimal_to_float2(sum(card_map.values()))

    total_revenue_plus_fees = decimal_to_float2(Decimal(str(investments_total)) + Decimal(str(cards_total)))

    return jsonify({
        "panel_title": "Revenue Panel",
        "mode": mode,
        "group": group,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "summary": {
            "label": "Total revenue + Transaction Fees",
            "investments_total": investments_total,
            "cards_total": cards_total,
            "total_revenue_plus_fees": total_revenue_plus_fees
        },
        "investments": {
            "plans": sorted(list(plans)),
            "series": investments_series,
            "total": investments_total
        },
        "cards": {
            "series": cards_series,
            "total": cards_total,
            "note": "Card revenue placeholder: data model to be added"
        }
    })

# ---------------- (Retro) Kleos Revenues mensili ----------------
@app.route("/api/kleos_profits", methods=["GET"])
def api_kleos_profits():
    start_m = request.args.get("start")
    end_m   = request.args.get("end")
    if not (start_m and end_m and ensure_month(start_m) and ensure_month(end_m)):
        today = datetime.now(TZ)
        end_m = today.strftime("%Y-%m")
        start_dt = (today.replace(day=1) - timedelta(days=5*31))
        start_m = start_dt.strftime("%Y-%m")

    if not (ensure_month(start_m) and ensure_month(end_m)):
        return jsonify({"error":"start/end must be YYYY-MM"}), 400

    rows = db.session.query(
        Distribution.month_ym,
        func.coalesce(func.sum(Distribution.payout_amount), 0)
    ).filter(
        Distribution.month_ym >= start_m,
        Distribution.month_ym <= end_m
    ).group_by(Distribution.month_ym).order_by(Distribution.month_ym.asc()).all()

    series = []
    total_investor_profit = Decimal("0")
    for ym, sum_pay in rows:
        pay = Decimal(str(sum_pay or 0))
        kleos = (pay * Decimal("0.20")).quantize(Decimal("0.01"))
        series.append({
            "month": ym,
            "investor_profit": float(pay),
            "kleos_profit": float(kleos)
        })
        total_investor_profit += pay

    total_kleos = (total_investor_profit * Decimal("0.20")).quantize(Decimal("0.01"))
    return jsonify({
        "start": start_m,
        "end": end_m,
        "total_investor_profit": float(total_investor_profit),
        "total_kleos_profit": float(total_kleos),
        "series": series
    })

@socketio.on("connect")
def on_connect():
    emit("connected", {"ok": True})

if __name__ == "__main__":
    with app.app_context():
        db.create_all()
        _ensure_epm_columns()   # garantisce colonne aggiuntive su SQLite
    if app.config["EXT_API_BASE"] and app.config["EXT_API_KEY"]:
        socketio.start_background_task(poll_loop)
    socketio.run(app, debug=True)
