from temporalio.client import Client
import asyncio
import uuid

async def main():
    client = await Client.connect("localhost:7233")

    workflow_id = f"dynamic-workflow-{uuid.uuid4()}"

    result = await client.start_workflow(
        "DynamicWorkflow",
        id=workflow_id,
        task_queue="dynamic-task-queue",
    )

    print(f"Workflow started with ID: {workflow_id}")

asyncio.run(main())
