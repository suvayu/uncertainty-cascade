#!/usr/bin/env python
"""Collate different metrics from Calliope runs in to a data package"""

from argparse import ArgumentParser
from pathlib import Path

import pkg_resources

from friendly_data.cli import _metadata
from friendly_data.converters import from_df
from friendly_data.dpkg import create_pkg, write_pkg, _resource

from errapids.metrics import ScenarioGroups, fraction_by_level

parser = ArgumentParser(description=__doc__)
parser.add_argument("datadir", help="directory with NetCDF files from Calliope run")
parser.add_argument("outdir", help="Directory where to write the datapackage")
parser.add_argument("--metadata", required=True, help="Config file with metadata")
parser.add_argument(
    "--tech-desc", required=True, help="CSV file with technology descriptions"
)

if __name__ == "__main__":
    opts = parser.parse_args()

    metrics = ScenarioGroups.from_dir(opts.datadir, "out_scenario*.nc")
    dfs = [metrics[name].to_frame() for name in metrics.varnames]
    prod_share = fraction_by_level(metrics["carrier_prod"], "technology")
    prod_share.name = "carrier_prod_share"
    dfs.append(prod_share.to_frame())
    resources = [
        from_df(df, basepath=opts.outdir, alias={"energy_cap": "nameplate_capacity"})
        for df in dfs
    ]

    meta = _metadata(["name", "licenses"], metadata=opts.metadata)
    meta.update(version=pkg_resources.require("errapids")[0].version)

    desc_in = Path(opts.tech_desc)
    desc_out = Path(opts.outdir) / desc_in.name
    desc_out.write_text(desc_in.read_text())
    resources.append(
        _resource({"path": f"{desc_in.name}"}, basepath=opts.outdir, infer=True)
    )
    pkg = create_pkg(meta, resources, basepath=opts.outdir, infer=False)
    write_pkg(pkg, opts.outdir)
