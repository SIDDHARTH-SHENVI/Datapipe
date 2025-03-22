import redis
import os
import json
from dotenv import load_dotenv
from .db import get_db_connection, init_db
from .pipeline import parse_pipeline, split_pipeline

load_dotenv()
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def queue_pipeline(pipeline_file):
    init_db()
    pipeline = parse_pipeline(pipeline_file)
    tasks = split_pipeline(pipeline)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO pipelines (name) VALUES (%s) RETURNING id;", (pipeline["name"],))
            pipeline_id = cur.fetchone()[0]
            for task in tasks:
                cur.execute(
                    "INSERT INTO tasks (pipeline_id, step, data) VALUES (%s, %s, %s) RETURNING id;",
                    (pipeline_id, task["step"], json.dumps(task["data"]))
                )
                task_id = cur.fetchone()[0]
                redis_client.rpush("task_queue", json.dumps({"id": task_id, "pipeline_id": pipeline_id}))
            conn.commit()
    return pipeline_id