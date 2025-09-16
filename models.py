# models.py
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from sqlalchemy import Numeric, UniqueConstraint

db = SQLAlchemy()

# ===== Registrazioni (investitori) =====
class Registration(db.Model):
    __tablename__ = "registrations"
    id = db.Column(db.Integer, primary_key=True)
    full_name = db.Column(db.String(200), nullable=False)
    plan = db.Column(db.String(1), nullable=False)  # 'A'|'B'|'C'
    amount = db.Column(Numeric(18, 2), nullable=False, default=0)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)

    def to_dict(self):
        return {
            "id": self.id, "full_name": self.full_name, "plan": self.plan,
            "amount": float(self.amount or 0), "created_at": self.created_at.isoformat()
        }

# ===== Rendimenti (config mese) =====
class MonthlyReturn(db.Model):
    __tablename__ = "monthly_returns"
    id = db.Column(db.Integer, primary_key=True)
    month_ym = db.Column(db.String(7), nullable=False, index=True)  # 'YYYY-MM'
    plan = db.Column(db.String(1), nullable=False)  # 'A'|'B'|'C'
    percent = db.Column(Numeric(6, 2), nullable=False, default=0)
    __table_args__ = (UniqueConstraint("month_ym", "plan", name="uq_month_plan"),)

class MonthlyResult(db.Model):
    __tablename__ = "monthly_results"
    id = db.Column(db.Integer, primary_key=True)
    month_ym = db.Column(db.String(7), nullable=False, unique=True, index=True)
    total_eur = db.Column(Numeric(18, 2), nullable=False, default=0)

# ===== Distribuzione (risultato calcolo mese) =====
class Distribution(db.Model):
    __tablename__ = "distributions"
    id = db.Column(db.Integer, primary_key=True)
    month_ym = db.Column(db.String(7), nullable=False, index=True)
    registration_id = db.Column(db.Integer, db.ForeignKey("registrations.id"), nullable=False, index=True)
    full_name = db.Column(db.String(200), nullable=False)
    plan = db.Column(db.String(1), nullable=False)
    invested_amount = db.Column(Numeric(18, 2), nullable=False)
    plan_share_percent = db.Column(Numeric(6, 2), nullable=False)
    effective_percent = db.Column(Numeric(10, 6), nullable=False)
    payout_amount = db.Column(Numeric(18, 2), nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)

    registration = db.relationship("Registration", backref="distributions")
    __table_args__ = (UniqueConstraint("month_ym", "registration_id", name="uq_month_registration"),)

    def to_dict(self):
        return {
            "id": self.id, "month_ym": self.month_ym, "registration_id": self.registration_id,
            "full_name": self.full_name, "plan": self.plan,
            "invested_amount": float(self.invested_amount or 0),
            "plan_share_percent": float(self.plan_share_percent or 0),
            "effective_percent": float(self.effective_percent or 0),
            "payout_amount": float(self.payout_amount or 0),
            "created_at": self.created_at.isoformat()
        }

# ===== Riallocazione a "contratti da 1 cent" =====
class UnitBalance(db.Model):
    __tablename__ = "unit_balances"
    id = db.Column(db.Integer, primary_key=True)
    registration_id = db.Column(db.Integer, db.ForeignKey("registrations.id"), nullable=False, unique=True, index=True)
    plan = db.Column(db.String(1), nullable=False)
    units = db.Column(db.Integer, nullable=False, default=0)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    registration = db.relationship("Registration", backref="unit_balance")

class Order(db.Model):
    __tablename__ = "orders"
    id = db.Column(db.Integer, primary_key=True)
    registration_id = db.Column(db.Integer, db.ForeignKey("registrations.id"), nullable=False, index=True)
    plan = db.Column(db.String(1), nullable=False)
    side = db.Column(db.String(3), nullable=False)  # 'IN'|'OUT'
    amount_eur = db.Column(Numeric(18, 2), nullable=False)
    units = db.Column(db.Integer, nullable=False)
    status = db.Column(db.String(12), nullable=False, default="PENDING")
    batch_id = db.Column(db.String(20), nullable=True, index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    registration = db.relationship("Registration")

    def to_dict(self):
        return {
            "id": self.id, "registration_id": self.registration_id, "plan": self.plan,
            "side": self.side, "amount_eur": float(self.amount_eur or 0), "units": self.units,
            "status": self.status, "batch_id": self.batch_id,
            "created_at": self.created_at.isoformat()
        }

class Transfer(db.Model):
    __tablename__ = "transfers"
    id = db.Column(db.Integer, primary_key=True)
    batch_id = db.Column(db.String(20), nullable=False, index=True)
    plan = db.Column(db.String(1), nullable=False)
    from_registration_id = db.Column(db.Integer, db.ForeignKey("registrations.id"), nullable=False)
    to_registration_id = db.Column(db.Integer, db.ForeignKey("registrations.id"), nullable=False)
    units = db.Column(db.Integer, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id, "batch_id": self.batch_id, "plan": self.plan,
            "from_registration_id": self.from_registration_id,
            "to_registration_id": self.to_registration_id,
            "units": self.units, "created_at": self.created_at.isoformat()
        }

# ===== NUOVO: Allocazioni di portafoglio per diversificazione =====
class HoldingAllocation(db.Model):
    """
    NAV allocato per combinazione plan/asset_class/strategy/gp.
    Usa questo per costruire i grafici di diversificazione.
    """
    __tablename__ = "holding_allocations"
    id = db.Column(db.Integer, primary_key=True)
    plan = db.Column(db.String(1), nullable=False)                # 'A'|'B'|'C'
    asset_class = db.Column(db.String(50), nullable=False)        # es. 'Fund' | 'RWA' | 'Cash'
    strategy = db.Column(db.String(100), nullable=False)          # es. 'Smart Yield', 'Credit', ...
    gp = db.Column(db.String(120), nullable=False)                # es. 'BlackRock Global Fund'
    nav_eur = db.Column(Numeric(18, 2), nullable=False, default=0)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
