"""Register the Trino (Iceberg) database connection in Superset if missing."""
from superset.app import create_app

app = create_app()
with app.app_context():
    from superset import db
    from superset.models.core import Database

    NAME = "Trino Iceberg"
    URI = "trino://trino@trino:8080/iceberg"

    existing = db.session.query(Database).filter_by(database_name=NAME).first()
    if existing:
        print(f"[superset] Database '{NAME}' already exists.")
    else:
        database = Database(database_name=NAME, sqlalchemy_uri=URI)
        database.allow_ctas = True
        database.allow_cvas = True
        database.allow_dml = False
        database.expose_in_sqllab = True
        db.session.add(database)
        db.session.commit()
        print(f"[superset] Registered database '{NAME}' -> {URI}")
