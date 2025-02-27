import asyncio
from temporalio import worker, client

from object.object_activity_workflow import ObjectActivityWorkflow
from object.task_processor import TaskProcessor


async def main():
    # Create Temporal client
    temporal_client = await client.Client.connect("localhost:7233")

    # Create an instance of TaskProcessor
    task_processor = TaskProcessor(prefix="Worker1")

    # Register the worker with the activity instance
    async with worker.Worker(
        temporal_client,
        task_queue="example-task-queue",
        workflows=[ObjectActivityWorkflow],
        activities=[task_processor.process],  # Register the method
    ):
        print("Worker started")
        await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
