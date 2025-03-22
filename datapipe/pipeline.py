import yaml

def parse_pipeline(file_path):
    with open(file_path, 'r') as f:
        return yaml.safe_load(f)

def split_pipeline(pipeline):
    return [{"step": step["type"], "data": step.get("data", {})} for step in pipeline["steps"]]