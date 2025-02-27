# trigger_seq_workflow.py
import asyncio
from temporalio import client

async def main():
    temporal_client = await client.Client.connect("localhost:7233")

    # Define message structure
    message = {
        "run_child": True,
        "child_payload": {"child_key": "child_value"},
        "tasks": ["task_a", "task_b", "task_c"]
    }

    # Start the Main Workflow
    result = await temporal_client.start_workflow(
        workflow="MainWorkflow",
        id="main-workflow-run",
        task_queue="demo-task-queue",
        args=[message],
    )

    print(f"Workflow Result: {result}")

if __name__ == "__main__":
    asyncio.run(main())
