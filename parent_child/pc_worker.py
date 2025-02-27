# pc_worker.py
import asyncio
from temporalio import worker
from temporalio.client import Client

from main_workflow import MainWorkflow
from child_workflow import ChildWorkflow
from pc_activities import process_task_a, process_task_b, process_task_c


async def main():
    # Connect to Temporal server
    client = await Client.connect("localhost:7233")

    # Start the worker
    async with worker.Worker(
            client,
            task_queue="demo-task-queue",
            workflows=[MainWorkflow, ChildWorkflow],
            activities=[process_task_a, process_task_b, process_task_c],
    ):
        print("Worker started. Listening for tasks...")
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
