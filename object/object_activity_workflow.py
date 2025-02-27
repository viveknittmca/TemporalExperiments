import asyncio
from datetime import timedelta

from temporalio import workflow

from object.task_processor import TaskProcessor


@workflow.defn
class ObjectActivityWorkflow:
    @workflow.run
    async def run(self, data_list: list) -> list:
        activity_futures = []

        for data in data_list:
            # Call the activity method with workflow.execute_activity
            future = workflow.execute_activity(
                TaskProcessor.process,  # Call the method
                data,  # Pass data to the activity
                start_to_close_timeout=timedelta(seconds=60)
            )
            activity_futures.append(future)

        # Wait for all activities to complete
        results = await asyncio.gather(*activity_futures)
        return results
