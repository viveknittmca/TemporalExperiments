from temporalio.worker import Worker
from temporalio.client import Client
import asyncio
from static_workflow import StaticWorkflow


async def main():
    client = await Client.connect("localhost:7233")  # Connect to Temporal Server

    worker = Worker(
        client,
        task_queue="my-task-queue",  # 👈 Task Queue Name (Matches Workflow)
        workflows=[StaticWorkflow],  # Register Workflow
        activities=[],
    )

    print("Worker started, listening for tasks...")
    await worker.run()


if __name__ == '__main__':
    asyncio.run(main())
