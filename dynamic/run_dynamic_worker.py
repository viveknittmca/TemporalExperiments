from temporalio import activity
from temporalio.worker import Worker
from temporalio.client import Client
import asyncio

from dynamic.discovery import discover_activities
from dynamic_workflow import DynamicWorkflow
import activities
# from activities import task1, task2, task3
import inspect
import activities  # 👈 Import the activities module dynamically


async def main():
    client = await Client.connect("localhost:7233")

    # all_activities = [
    #     func for name, func in inspect.getmembers(activities, inspect.isfunction)
    # ]
    #     if inspect.isfunction(func) and hasattr(func, "_activity_definition")
    all_activities = discover_activities(activities)
    print(all_activities)

    # all_activities = [
    #     getattr(activities, name) for name in dir(activities)
    # ]
    # # if callable(getattr(activities, name)) and hasattr(getattr(activities, name), "_activity_definition")
    # print(all_activities)

    worker = Worker(
        client,
        task_queue="dynamic-task-queue",  # 👈 Must match workflow execution
        workflows=[DynamicWorkflow],
        # activities=[task1, task2, task3],  # 👈 Register all possible tasks
        activities=all_activities
    )

    print("Worker started, listening for dynamic workflows...")
    await worker.run()


if __name__ == '__main__':
    asyncio.run(main())
