from temporalio.client import Client
import asyncio
import uuid  # Generate unique workflow ID


async def main():
    client = await Client.connect("localhost:7233")  # Connect to Temporal Server

    result = await client.start_workflow(
        "StaticWorkflow",  # 👈 Workflow method name
        "Hello, Temporal!",  # 👈 Input data
        id=f"workflow-{uuid.uuid4()}",  # 👈 Unique workflow execution ID
        task_queue="my-task-queue",  # 👈 Must match worker task queue
    )

    print(f"Workflow started with ID: {result}")


if __name__ == '__main__':
    asyncio.run(main())
