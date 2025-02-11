from temporalio import activity


@activity.defn
async def monte_carlo_task(name: str):
    return f"Executed MonteCarlo for {name}"