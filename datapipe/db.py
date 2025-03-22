import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST", "localhost")
    )

def init_db():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pipelines (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    status TEXT DEFAULT 'PENDING'
                );
                CREATE TABLE IF NOT EXISTS tasks (
                    id SERIAL PRIMARY KEY,
                    pipeline_id INTEGER REFERENCES pipelines(id),
                    step TEXT,
                    data TEXT,
                    status TEXT DEFAULT 'PENDING'
                );
            """)
            conn.commit()

def get_pipeline_status(pipeline_id):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM pipelines WHERE id = %s;", (pipeline_id,))
            return cur.fetchone()[0] if cur.rowcount else "Not Found"