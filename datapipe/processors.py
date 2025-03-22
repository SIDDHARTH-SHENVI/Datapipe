import pandas as pd
import json
from .db import get_db_connection

processors = {
    "read_csv": lambda data: pd.read_csv(data["path"]).to_dict(orient="records"),
    "filter": lambda data, input_data: [row for row in input_data if eval(data["condition"], {"row": row})],
    "aggregate": lambda data, input_data: pd.DataFrame(input_data).agg(data["func"]).to_dict(),
    "write_csv": lambda data, input_data: pd.DataFrame([input_data] if isinstance(input_data, dict) else input_data).to_csv(data["path"], index=False)
}

def process_task(task):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT step, data FROM tasks WHERE id = %s;", (task["id"],))
            step, task_data = cur.fetchone()
            task_data = json.loads(task_data)

            cur.execute(
                "SELECT data FROM tasks WHERE pipeline_id = %s AND id < %s AND status = 'COMPLETED' ORDER BY id DESC LIMIT 1;",
                (task["pipeline_id"], task["id"])
            )
            prev_data = cur.fetchone()
            input_data = json.loads(prev_data[0]) if prev_data else None

            result = processors[step](task_data, input_data) if input_data else processors[step](task_data)
            cur.execute("UPDATE tasks SET data = %s WHERE id = %s;", (json.dumps(result), task["id"]))
            conn.commit()