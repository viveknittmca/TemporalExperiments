# main_workflow.py
from temporalio import workflow
from pc_activities import process_task_a, process_task_b, process_task_c
from child_workflow import ChildWorkflow
from datetime import timedelta


@workflow.defn
class MainWorkflow:
    @workflow.run
    async def run(self, message: dict) -> str:
        results = []

        # Extract the workflow and activity sequence from the message
        tasks = message.get("tasks", [])
        child_payload = message.get("child_payload", {})

        # Run Child Workflow if needed
        if message.get("run_child", False):
            child_result = await workflow.execute_child_workflow(
                ChildWorkflow.run, child_payload
            )
            results.append(child_result)

        # Run activities based on message sequence
        for task in tasks:
            if task == "task_a":
                result = await workflow.execute_activity(
                    process_task_a, {"key": "value"}, start_to_close_timeout=timedelta(seconds=30)
                )
            elif task == "task_b":
                result = await workflow.execute_activity(
                    process_task_b, {"key": "value"}, start_to_close_timeout=timedelta(seconds=30)
                )
            elif task == "task_c":
                result = await workflow.execute_activity(
                    process_task_c, {"key": "value"}, start_to_close_timeout=timedelta(seconds=30)
                )
            else:
                result = f"Unknown task: {task}"
            results.append(result)

        return f"Main Workflow completed with results: {results}"
