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


def prepare_sarek_v2_launch_info(dataset: Dataset) -> LaunchInfo:
    """Generate LaunchInfo for nf-core/sarek v2 workflow run."""
    run_name = dataset.get_run_name("sarek_v2")

    params = {
        "input": dataset.samplesheet,
        "outdir": f"s3://ntap-add5-project-tower-bucket/outputs/{run_name}/",
        "step": dataset.starting_step,
        "tools": "DeepVariant,FreeBayes,Mutect2,Strelka",
        "model_type": "WGS",
        "genome": "GRCh38",
        "igenomes_base": "s3://sage-igenomes/igenomes",
    }

    nextflow_config = """
    process {
        withLabel:deepvariant {
            cpus = 24
        }
    }
    """

    return LaunchInfo(
        run_name=run_name,
        pipeline="Sage-Bionetworks-Workflows/sarek",
        revision="d48de18fc5342c02c64c0e0b0e704012b28c91dd",
        profiles=["sage"],
        params=params,
        nextflow_config=dedent(nextflow_config),
    )


def prepare_sarek_v3_launch_info(dataset: Dataset) -> LaunchInfo:
    """Generate LaunchInfo for nf-core/sarek v3 workflow run."""
    run_name = dataset.get_run_name("sarek_v3")

    params = {
        "input": dataset.samplesheet,
        "step": dataset.starting_step,
        "wes": False,
        "igenomes_base": "s3://sage-igenomes/igenomes",
        "genome": "GATK.GRCh38",
        "tools": "cnvkit",
        "outdir": f"s3://ntap-add5-project-tower-bucket/outputs/{run_name}/",
    }

    return LaunchInfo(
        run_name=run_name,
        pipeline="Sage-Bionetworks-Workflows/sarek",
        revision="3.1.2-safe",
        profiles=["sage"],
        params=params,
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
    if dataset.starting_step == "mapping":
        sarek_info = prepare_sarek_v2_launch_info(dataset)
    elif dataset.starting_step == "variant_calling":
        sarek_info = prepare_sarek_v3_launch_info(dataset)
    else:
        raise ValueError("Unexpected starting step")
    sarek_run_id = ops.launch_workflow(sarek_info, "spot")
    await monitor_run(ops, sarek_run_id)

    # TODO: Generate Sarek v3 sample sheet from v2 run
    # if dataset.starting_step == "mapping":
    #     sarek_v3_info = prepare_sarek_v3_launch_info(dataset)
    #     sarek_run_id = ops.launch_workflow(sarek_info, "spot")
    #     await monitor_run(ops, sarek_run_id)

    # synindex_info = prepare_synindex_launch_info(dataset)
    # synindex_run_id = ops.launch_workflow(synindex_info, "spot")
    # await monitor_run(ops, synindex_run_id)


if __name__ == "__main__":
    configure_logging()
    asyncio.run(main())
