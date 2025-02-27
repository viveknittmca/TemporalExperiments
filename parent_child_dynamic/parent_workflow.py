import asyncio
from temporalio import workflow
from child_workflows import CheckoutWorkflow, PaymentWorkflow, OrderCreationWorkflow


@workflow.defn
class DynamicSequentialWorkflowExecutor:
    @workflow.run
    async def run(self, message: dict) -> dict:
        results = {}

        # Define available child workflows
        workflow_mapping = {
            "CheckoutWorkflow": CheckoutWorkflow.run,
            "PaymentWorkflow": PaymentWorkflow.run,
            "OrderCreationWorkflow": OrderCreationWorkflow.run,
        }

        try:
            for workflow_name in message["tasks"]:
                if workflow_name in workflow_mapping:
                    result = await workflow.execute_child_workflow(
                        workflow_mapping[workflow_name], message["data"]
                    )
                    results[workflow_name] = result
                else:
                    results[workflow_name] = "Unknown Workflow"

            return {"status": "Success", "results": results}

        except Exception as e:
            return {"status": "Failure", "error": str(e)}


@workflow.defn
class DynamicParallelWorkflowExecutor:
    @workflow.run
    async def run(self, sequence: list, data: dict) -> dict:
        results = {}

        # Define available child workflows
        workflow_mapping = {
            "CheckoutWorkflow": CheckoutWorkflow.run,
            "PaymentWorkflow": PaymentWorkflow.run,
            "OrderCreationWorkflow": OrderCreationWorkflow.run,
        }

        try:
            child_futures = [
                workflow.execute_child_workflow(workflow_mapping[workflow_name], data)
                for workflow_name in sequence if workflow_name in workflow_mapping
            ]

            results = await asyncio.gather(*child_futures)
            return results
        except Exception as e:
            return {"status": "Failure", "error": str(e)}

