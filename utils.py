import asyncio

from orca.services.nextflowtower import NextflowTowerOps


async def monitor_run(ops: NextflowTowerOps, run_id: str):
    """Wait until workflow run completes."""
    workflow = ops.get_workflow(run_id)
    print(f"Starting to monitor workflow: {workflow.run_name} ({run_id})")

    status, is_done = ops.get_workflow_status(run_id)
    while not is_done:
        print(f"Not done yet. Status is '{status.value}'. Checking again in 5 min...")
        await asyncio.sleep(60 * 5)
        status, is_done = ops.get_workflow_status(run_id)

    print(f"Done! Final status is '{status.value}'.")
    return status
