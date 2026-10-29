"""Acces aux bases + enregistrement des resultats de controle."""
import json
import os

import psycopg2
import pymysql


def pg_conn():
    return psycopg2.connect(os.environ["PG_DSN"])


def mysql_conn():
    return pymysql.connect(
        host=os.environ.get("MYSQL_HOST", "mysql"),
        port=int(os.environ.get("MYSQL_PORT", "3306")),
        user=os.environ.get("MYSQL_USER", "root"),
        password=os.environ.get("MYSQL_PASSWORD", ""),
        database=os.environ.get("MYSQL_DB", "shop"),
    )


def pg_scalar(sql: str):
    with pg_conn() as c, c.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


def mysql_scalar(sql: str):
    conn = mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0]
    finally:
        conn.close()


def record_check(run_id: str, check_name: str, passed: bool, metadata: dict) -> None:
    """Journalise le resultat dans quality_results (lu par l'API)."""
    with pg_conn() as c, c.cursor() as cur:
        cur.execute(
            "INSERT INTO public.quality_results (run_id, check_name, passed, metadata) "
            "VALUES (%s, %s, %s, %s)",
            (run_id, check_name, passed, json.dumps(metadata, default=str)),
        )
