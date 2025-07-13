#!/usr/bin/env python3
"""
Database setup for FastMCP examples (PostgreSQL edition).

Crea una base de datos de ejemplo de e-commerce con tablas users y orders.
"""

import os
from datetime import datetime
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    Numeric,           # Numeric es la versión agnóstica de DECIMAL
    text,
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

# ───────────────────────────────────────────────────────────────
#  Configuración: usa variables de entorno para no hard-codear
# ───────────────────────────────────────────────────────────────
#
#   export PG_USER=postgres
#   export PG_PASSWORD=secret
#   export PG_HOST=localhost
#   export PG_PORT=5432
#   export PG_DB=mydb
#
# o bien cambia la URL directamente en create_engine(...)
# ───────────────────────────────────────────────────────────────

PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "secret")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "postgres")

DATABASE_URL = (
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

Base = declarative_base()

# ───────────────────────────────────────────────────────────────
#  Modelos
# ───────────────────────────────────────────────────────────────


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    email = Column(String, unique=True, nullable=False)
    age = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)

    orders = relationship("Order", back_populates="user")


class Order(Base):
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id"))
    product_name = Column(String, nullable=False)
    quantity = Column(Integer, default=1)
    price = Column(Numeric(10, 2))
    order_date = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", back_populates="orders")


# ───────────────────────────────────────────────────────────────
#  Función de carga de datos
# ───────────────────────────────────────────────────────────────


def create_sample_database(db_url: str = DATABASE_URL):
    """
    Crea las tablas y carga datos de ejemplo en PostgreSQL.

    Si las tablas existen, las vacía antes de insertar los registros
    para garantizar idempotencia.
    """
    engine = create_engine(db_url, echo=False)

    # Crear tablas si no existen
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)

    with Session() as session:
        # Vaciar tablas (opcional, para reiniciar)
        session.execute(text("TRUNCATE TABLE orders RESTART IDENTITY CASCADE"))
        session.execute(text("TRUNCATE TABLE users RESTART IDENTITY CASCADE"))

        # Usuarios
        users = [
            User(name="Alice Johnson", email="alice@example.com", age=28),
            User(name="Bob Smith", email="bob@example.com", age=35),
            User(name="Charlie Brown", email="charlie@example.com", age=22),
            User(name="Diana Prince", email="diana@example.com", age=30),
            User(name="Edward Davis", email="edward@example.com", age=45),
        ]
        session.add_all(users)
        session.flush()  # Para obtener IDs generados automáticamente

        # Pedidos
        orders = [
            Order(user_id=users[0].id, product_name="Laptop", quantity=1, price=999.99),
            Order(user_id=users[0].id, product_name="Mouse", quantity=2, price=29.99),
            Order(user_id=users[1].id, product_name="Keyboard", quantity=1, price=79.99),
            Order(user_id=users[2].id, product_name="Monitor", quantity=1, price=299.99),
            Order(user_id=users[2].id, product_name="Webcam", quantity=1, price=89.99),
            Order(user_id=users[3].id, product_name="Headphones", quantity=1, price=149.99),
            Order(user_id=users[4].id, product_name="Tablet", quantity=1, price=499.99),
            Order(user_id=users[4].id, product_name="Charger", quantity=3, price=24.99),
        ]
        session.add_all(orders)

        session.commit()

    print("✅ PostgreSQL e-commerce sample loaded successfully")
    return db_url


# ───────────────────────────────────────────────────────────────
#  Ejecutar como script
# ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    create_sample_database()
