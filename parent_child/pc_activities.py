# pc_activities.py
from temporalio import activity
import time


@activity.defn
async def process_task_a(data: dict) -> str:
    time.sleep(2)  # Simulate some processing
    return f"Task A processed with data: {data}"


@activity.defn
async def process_task_b(data: dict) -> str:
    time.sleep(2)
    return f"Task B processed with data: {data}"


@activity.defn
async def process_task_c(data: dict) -> str:
    time.sleep(2)
    return f"Task C processed with data: {data}"
