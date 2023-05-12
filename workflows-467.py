#!/usr/bin/env python3

import asyncio
from dataclasses import dataclass
from textwrap import dedent

from orca.services.nextflowtower import NextflowTowerOps
from orca.services.nextflowtower.models import LaunchInfo
from yaml import safe_load

from utils import configure_logging, monitor_run


async def main():
    ops = NextflowTowerOps()
    datasets = generate_datasets()
    runs = [run_workflows(ops, dataset) for dataset in datasets]
    statuses = await asyncio.gather(*runs)
    print(statuses)


@dataclass
class Dataset:
    id: str
    starting_step: str
    samplesheet: str
    parent_id: str

    def get_run_name(self, prefix: str):
        return f"{prefix}_{self.id}"


def generate_datasets():
    """Generate datasets objects from YAML file."""
    with open("workflows-467.yaml", "r") as fp:
        datasets_raw = safe_load(fp)
    return [Dataset(**kwargs) for kwargs in datasets_raw]


def prepare_sarek_launch_info(dataset: Dataset) -> LaunchInfo:
    """Generate LaunchInfo for nf-core/sarek workflow run."""
    run_name = dataset.get_run_name("sarek")

    if dataset.starting_step == "variant_calling":
        tools = "cnvkit"
    else:
        tools = "cnvkit,deepvariant,freebayes,mutect2,strelka"

    params = {
        "input": dataset.samplesheet,
        "step": dataset.starting_step,
        "wes": False,
        "igenomes_base": "s3://sage-igenomes/igenomes",
        "genome": "GATK.GRCh38",
        "tools": tools,
        "outdir": f"s3://ntap-add5-project-tower-bucket/outputs/{run_name}/",
    }

    nextflow_config = """
    model_type = params.wes ? "WES" : "WGS"

    process {
        withName: DEEPVARIANT {
            ext.args = "--model_type ${{model_type}}"
        }
    }
    """

    return LaunchInfo(
        run_name=run_name,
        pipeline="nf-core/sarek",
        revision="3.1.2",
        profiles=["sage"],
        params=params,
        nextflow_config=dedent(nextflow_config),
    )


def prepare_synindex_launch_info(dataset: Dataset) -> LaunchInfo:
    """Generate LaunchInfo for nf-synindex workflow run."""
    synindex_run_name = dataset.get_run_name("synindex")
    sarek_run_name = dataset.get_run_name("sarek")

    return LaunchInfo(
        run_name=synindex_run_name,
        pipeline="Sage-Bionetworks-Workflows/nf-synindex",
        revision="main",
        profiles=["sage"],
        params={
            "s3_prefix": f"s3://ntap-add5-project-tower-bucket/outputs/{sarek_run_name}/",
            "parent_id": dataset.parent_id,
        },
        workspace_secrets=["SYNAPSE_AUTH_TOKEN"],
    )


async def run_workflows(ops: NextflowTowerOps, dataset: Dataset):
    sarek_info = prepare_sarek_launch_info(dataset)
    sarek_run_id = ops.launch_workflow(sarek_info, "spot")
    await monitor_run(ops, sarek_run_id)

    # synindex_info = prepare_synindex_launch_info(dataset)
    # synindex_run_id = ops.launch_workflow(synindex_info, "spot")
    # await monitor_run(ops, synindex_run_id)


if __name__ == "__main__":
    configure_logging()
    asyncio.run(main())
