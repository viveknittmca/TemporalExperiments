import asyncio
from temporalio import client

from parent_child_dynamic.parent_workflow import DynamicSequentialWorkflowExecutor


async def trigger():
    temporal_client = await client.Client.connect("localhost:7233")

    # Define a dynamic sequence
    message1 = {"data": {"user_id": 123, "ip": "1.2.3.4"}, "tasks": ["CheckoutWorkflow", "PaymentWorkflow", "OrderCreationWorkflow"]}
    message2 = {"data": {"user_id": 456, "ip": "5.6.7.8"}, "tasks":  ["CheckoutWorkflow", "OrderCreationWorkflow", "PaymentWorkflow"]}

    # Start workflows with different sequences
    result1 = await temporal_client.start_workflow(
        workflow=DynamicSequentialWorkflowExecutor.run,
        id="dynamic-workflow-run-1",
        task_queue="dynamic-sequential-workflow-queue",
        args=[message1, message2]
    )

    # result2 = await temporal_client.start_workflow(
    #     workflow=DynamicSequentialWorkflowExecutor.run,
    #     id="dynamic-workflow-run-2",
    #     task_queue="dynamic-sequential-workflow-queue",
    #     args=[message2]
    # )

    print("Workflow 1 Result:", result1)
    # print("Workflow 2 Result:", result2)

if __name__ == "__main__":
    asyncio.run(trigger())


# workflow="MainWorkflow",
#         id="main-workflow-run",
#         task_queue="demo-task-queue",
#         args=[message],