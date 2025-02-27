import asyncio
from temporalio import client

from object.object_activity_workflow import ObjectActivityWorkflow


async def trigger_workflow():
    temporal_client = await client.Client.connect("localhost:7233")

    # Data list to process
    data_list = ["task1", "task2", "task3"]

    # Start the workflow
    result = await temporal_client.start_workflow(
        ObjectActivityWorkflow.run,
        data_list,
        id="object-activity-workflow",
        task_queue="example-task-queue",
    )

    print("Workflow Result:", result)

if __name__ == "__main__":
    asyncio.run(trigger_workflow())
