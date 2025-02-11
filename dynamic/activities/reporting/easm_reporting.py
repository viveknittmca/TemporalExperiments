from temporalio import activity


@activity.defn
async def easm_reporting_task(name: str):
    return f"Executed EASM Reporting for {name}"