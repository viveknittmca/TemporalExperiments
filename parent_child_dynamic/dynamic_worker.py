import asyncio
from temporalio import worker, client
from parent_workflow import DynamicSequentialWorkflowExecutor
from child_workflows import CheckoutWorkflow, PaymentWorkflow, OrderCreationWorkflow


async def main():
    temporal_client = await client.Client.connect("localhost:7233")

    async with worker.Worker(
            temporal_client,
            task_queue="dynamic-sequential-workflow-queue",
            workflows=[DynamicSequentialWorkflowExecutor, CheckoutWorkflow, PaymentWorkflow, OrderCreationWorkflow]
    ):
        print("Worker started")
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
