import argparse
from rich.console import Console
from .queue import queue_pipeline
from .worker import start_worker
from .db import get_pipeline_status

console = Console()

def main():
    parser = argparse.ArgumentParser(description="DataPipe: Distributed Data Processing")
    subparsers = parser.add_subparsers(dest="command")

    queue_parser = subparsers.add_parser("queue", help="Queue a pipeline")
    queue_parser.add_argument("pipeline_file", help="Path to pipeline YAML")

    subparsers.add_parser("worker", help="Start a worker")

    status_parser = subparsers.add_parser("status", help="Check pipeline status")
    status_parser.add_argument("pipeline_id", type=int, help="Pipeline ID")

    args = parser.parse_args()

    if args.command == "queue":
        pipeline_id = queue_pipeline(args.pipeline_file)
        console.print(f"[green]✅ Pipeline queued with ID {pipeline_id}[/]")
    elif args.command == "worker":
        start_worker()
    elif args.command == "status":
        status = get_pipeline_status(args.pipeline_id)
        console.print(f"[blue]Pipeline {args.pipeline_id}: {status}[/]")
    else:
        parser.print_help()