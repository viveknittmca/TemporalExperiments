from temporalio.client import Client
import asyncio
import uuid


async def main():
    client = await Client.connect("localhost:7233")  # Connect to Temporal Server

    # Simulating a message containing dynamic tasks
    message = {
        "workflow_id": f"dynamic-workflow-{uuid.uuid4()}",
        "tasks": ["easm_reporting_task", "monte_carlo_task"]  # Task names coming from the message
    }

    result = await client.start_workflow(
        "DynamicWorkflow",
        message["tasks"],
        id=message["workflow_id"],
        task_queue="dynamic-task-queue",
    )

    print(f"Workflow started with ID: {message['workflow_id']}")


if __name__ == '__main__':
    asyncio.run(main())
