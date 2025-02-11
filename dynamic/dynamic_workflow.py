from datetime import timedelta

from temporalio import workflow


@workflow.defn
class DynamicWorkflow:
    @workflow.run
    async def run(self, tasks: list) -> str:
        results = []
        for task in tasks:
            print(f"Workflow running the task: {task}")
            result = await workflow.execute_activity(task, task, start_to_close_timeout=timedelta(seconds=30))
            results.append(result)
        return f"Workflow completed with results: {results}"

