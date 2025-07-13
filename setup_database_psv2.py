#!/usr/bin/env python3
"""
Database setup for a **Logistics Quotation** demo (PostgreSQL edition).

Estructura mínima para un *cotizador logístico* capaz de tarificar
por:
    • Tipo de almacén (warehouse_type)
    • Sede / Almacén específico (warehouse)
    • Tipo de servicio (service_type)
    • Tipo de producto / categoría (product_type)

Produce tres tablas maestras y las tablas de cotización:
    * warehouses, warehouse_types
    * service_types
    * product_types
    * quotations (cabecera) + quotation_items (detalle)

La función create_sample_database() carga datos de ejemplo para probar
consultas rápidamente.
"""

import os
from datetime import datetime
from decimal import Decimal
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    Numeric,
    text,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# ───────────────────────── Config env ──────────────────────────────
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "secret")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "postgres")

DATABASE_URL = (
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

Base = declarative_base()

# ──────────────────────────── Models ───────────────────────────────

class WarehouseType(Base):
    __tablename__ = "warehouse_types"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)  # e.g. Seco, Refrigerado

    warehouses = relationship("Warehouse", back_populates="warehouse_type")

class Warehouse(Base):
    __tablename__ = "warehouses"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)  # e.g. Lima‑Ate DC‑01
    location = Column(String, nullable=False)  # ciudad o sede
    warehouse_type_id = Column(Integer, ForeignKey("warehouse_types.id"))

    warehouse_type = relationship("WarehouseType", back_populates="warehouses")
    quotations = relationship("Quotation", back_populates="warehouse")

class ServiceType(Base):
    __tablename__ = "service_types"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)  # e.g. Almacenaje, Picking

    quotations = relationship("Quotation", back_populates="service_type")

class ProductType(Base):
    __tablename__ = "product_types"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)  # e.g. Perecible, Textil

    quotation_items = relationship("QuotationItem", back_populates="product_type")

class Quotation(Base):
    __tablename__ = "quotations"
    id = Column(Integer, primary_key=True)
    customer_name = Column(String, nullable=False)
    warehouse_id = Column(Integer, ForeignKey("warehouses.id"))
    service_type_id = Column(Integer, ForeignKey("service_types.id"))
    created_at = Column(DateTime, default=datetime.utcnow)

    warehouse = relationship("Warehouse", back_populates="quotations")
    service_type = relationship("ServiceType", back_populates="quotations")
    items = relationship("QuotationItem", back_populates="quotation", cascade="all, delete-orphan")

class QuotationItem(Base):
    __tablename__ = "quotation_items"
    id = Column(Integer, primary_key=True)
    quotation_id = Column(Integer, ForeignKey("quotations.id"))
    product_type_id = Column(Integer, ForeignKey("product_types.id"))
    volume_cbm = Column(Numeric(12, 3))  # volumen en metros cúbicos
    weight_kg = Column(Numeric(12, 3))
    cost_usd = Column(Numeric(10, 2))

    quotation = relationship("Quotation", back_populates="items")
    product_type = relationship("ProductType", back_populates="quotation_items")

# ─────────────────────── Sample load func ──────────────────────────

def create_sample_database(db_url: str = DATABASE_URL):
    """Crea esquema y datos de ejemplo para el cotizador logístico."""
    engine = create_engine(db_url, echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    with Session() as session:
        # Limpiar (idempotente)
        for tbl in [QuotationItem, Quotation, ProductType, ServiceType, Warehouse, WarehouseType]:
            session.execute(text(f"TRUNCATE TABLE {tbl.__tablename__} RESTART IDENTITY CASCADE"))

        # Maestros
        wt_seco = WarehouseType(name="Seco")
        wt_refrigerado = WarehouseType(name="Refrigerado")
        session.add_all([wt_seco, wt_refrigerado])

        wh1 = Warehouse(name="DC‑Ate", location="Lima", warehouse_type=wt_seco)
        wh2 = Warehouse(name="Frío‑Callao", location="Callao", warehouse_type=wt_refrigerado)
        session.add_all([wh1, wh2])

        st_storage = ServiceType(name="Almacenaje")
        st_picking = ServiceType(name="Picking")
        session.add_all([st_storage, st_picking])

        pt_perecible = ProductType(name="Perecible")
        pt_textil = ProductType(name="Textil")
        session.add_all([pt_perecible, pt_textil])

        session.flush()

        # Cotizaciones
        q1 = Quotation(customer_name="Cliente A", warehouse=wh1, service_type=st_storage)
        q1.items.append(QuotationItem(product_type=pt_textil, volume_cbm=Decimal("12.5"), weight_kg=Decimal("300"), cost_usd=Decimal("450.00")))
        q1.items.append(QuotationItem(product_type=pt_perecible, volume_cbm=Decimal("5.0"), weight_kg=Decimal("120"), cost_usd=Decimal("280.00")))

        q2 = Quotation(customer_name="Cliente B", warehouse=wh2, service_type=st_picking)
        q2.items.append(QuotationItem(product_type=pt_perecible, volume_cbm=Decimal("8.0"), weight_kg=Decimal("200"), cost_usd=Decimal("390.00")))

        session.add_all([q1, q2])
        session.commit()

    print("✅ Logistics quotation sample DB loaded.")
    return db_url

# ────────────────────────── run as script ─────────────────────────
if __name__ == "__main__":
    create_sample_database()
