import os
import subprocess
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text


ROOT = Path(__file__).resolve().parents[1]
SCHEMA = ROOT / "sql" / "taxonomy" / "schema.sql"
VIEWS = ROOT / "sql" / "taxonomy" / "views.sql"
FIXTURE = ROOT / "data" / "external" / "taxonomy_compass" / "fixtures" / "compass_min.csv"
INGEST = ROOT / "etl" / "taxonomy" / "ingest_taxonomy_compass.py"


def _psql_url(database_url: str) -> str:
    return database_url.replace("postgresql+psycopg2", "postgresql")


@pytest.fixture(scope="module")
def db_engine():
    url = os.environ.get("DATABASE_URL")
    if not url:
        pytest.skip("DATABASE_URL no definido para tests")
    engine = create_engine(url, future=True)
    # aplicar schema
    with engine.begin() as conn:
        conn.exec_driver_sql(SCHEMA.read_text(encoding="utf-8"))
    yield engine
    # limpieza
    with engine.begin() as conn:
        conn.exec_driver_sql("DROP VIEW IF EXISTS eligibility_by_nace;")
        conn.exec_driver_sql("DROP TABLE IF EXISTS legal_reference CASCADE;")
        conn.exec_driver_sql("DROP TABLE IF EXISTS criteria_ms CASCADE;")
        conn.exec_driver_sql("DROP TABLE IF EXISTS criteria_dnsh CASCADE;")
        conn.exec_driver_sql("DROP TABLE IF EXISTS criteria_sc CASCADE;")
        conn.exec_driver_sql("DROP TABLE IF EXISTS activity_nace CASCADE;")
        conn.exec_driver_sql("DROP TABLE IF EXISTS taxonomy_activity CASCADE;")
        conn.exec_driver_sql("DROP TABLE IF EXISTS taxonomy_objective CASCADE;")
        conn.exec_driver_sql("DROP TABLE IF EXISTS release CASCADE;")


def test_ingest_and_view(db_engine):
    assert FIXTURE.exists(), "Fixture missing"
    env = os.environ.copy()
    assert env.get("DATABASE_URL"), "DATABASE_URL no definido"
    # Ejecutar ETL con fixtures
    cmd = [
        "python",
        str(INGEST),
        "--source",
        "csv",
        "--path",
        str(FIXTURE),
        "--database-url",
        env["DATABASE_URL"],
        "--release-name",
        "Compass_min",
        "--source-url",
        "fixtures://compass_min",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        print(res.stdout)
        print(res.stderr)
    assert res.returncode == 0, "ETL failed"

    # Crear vista
    with db_engine.begin() as conn:
        conn.exec_driver_sql(VIEWS.read_text(encoding="utf-8"))

    # Consultar vista por un NACE concreto
    with db_engine.connect() as conn:
        rows = conn.execute(text("SELECT * FROM eligibility_by_nace WHERE nace_code=:c"), {"c": "D35.11"}).fetchall()
        assert len(rows) >= 1
        # Validar que hay arrays y conteos coherentes
        r0 = rows[0]
        # r0: (nace_code, activity_id, title, sc_ids, sc_count, dnsh_ids, dnsh_count)
        assert isinstance(r0.sc_criteria_ids, list) or r0.sc_criteria_ids is None
        assert isinstance(r0.dnsh_criteria_ids, list) or r0.dnsh_criteria_ids is None
        assert r0.sc_count >= 0
        assert r0.dnsh_count >= 0

    # Comprobar FKs válidas con un join básico
    with db_engine.connect() as conn:
        cnt = conn.execute(
            text(
                """
                SELECT count(*) FROM taxonomy_activity a
                JOIN taxonomy_objective o ON o.id = a.objective_id
                JOIN release r ON r.id = o.release_id
                """
            )
        ).scalar_one()
        assert cnt >= 2

