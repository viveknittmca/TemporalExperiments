from temporalio import workflow


@workflow.defn
class StaticWorkflow:
    @workflow.run
    async def run(self, input_data: str) -> str:
        print(f"Workflow started with input: {input_data}")
        result = f"Processed {input_data}"
        return result
