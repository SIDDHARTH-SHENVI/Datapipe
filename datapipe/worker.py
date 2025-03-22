import redis
import os
import time
import json  # Add this line
from dotenv import load_dotenv
from rich.console import Console
from .db import get_db_connection
from .processors import process_task

load_dotenv()
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
console = Console()

def start_worker():
    console.print("[bold green]🚀 Worker started...[/]")
    while True:
        try:
            task_data = redis_client.blpop("task_queue", timeout=10)
            if task_data:
                task = json.loads(task_data[1])  # This line needs json
                console.print(f"[yellow]⚙️ Processing task {task['id']} for pipeline {task['pipeline_id']}[/]")
                process_task(task)
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("UPDATE tasks SET status = 'COMPLETED' WHERE id = %s;", (task["id"],))
                        cur.execute(
                            "UPDATE pipelines SET status = 'COMPLETED' WHERE id = %s AND NOT EXISTS "
                            "(SELECT 1 FROM tasks WHERE pipeline_id = %s AND status != 'COMPLETED');",
                            (task["pipeline_id"], task["pipeline_id"])
                        )
                        conn.commit()
                console.print(f"[green]✅ Task {task['id']} completed[/]")
            else:
                console.print("[dim]⏳ No tasks, waiting...[/]")
        except Exception as e:
            console.print(f"[red]❌ Error: {e}[/]")
            time.sleep(1)