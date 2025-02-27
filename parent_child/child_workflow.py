# child_workflow.py
from temporalio import workflow


@workflow.defn
class ChildWorkflow:
    @workflow.run
    async def run(self, payload: dict) -> str:
        # Perform operations specific to child workflow
        return f"Child Workflow completed with: {payload}"
