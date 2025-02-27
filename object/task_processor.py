from temporalio import activity


class TaskProcessor:
    def __init__(self, prefix):
        self.prefix = prefix

    @activity.defn
    async def process(self, data: str) -> str:
        return f"{self.prefix}: Processed {data}"
