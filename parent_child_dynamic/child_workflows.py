from temporalio import workflow


@workflow.defn
class CheckoutWorkflow:
    @workflow.run
    async def run(self, data: dict) -> str:
        return f"Checkout completed for {data}"


@workflow.defn
class PaymentWorkflow:
    @workflow.run
    async def run(self, data: dict) -> str:
        return f"Payment processed for {data}"


@workflow.defn
class OrderCreationWorkflow:
    @workflow.run
    async def run(self, data: dict) -> str:
        return f"Order created for {data}"
