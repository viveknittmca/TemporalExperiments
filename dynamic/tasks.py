from temporalio import activity


@activity.defn
async def task1(name: str):
    return f"Executed {name}"


@activity.defn
async def task2(name: str):
    return f"Executed {name}"


@activity.defn
async def task3(name: str):
    return f"Executed {name}"
