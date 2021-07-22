#!/usr/bin/env python
"""Collate different metrics from Calliope runs in to a data package"""

from argparse import ArgumentParser
from pathlib import Path

import pkg_resources

from friendly_data.cli import _metadata
from friendly_data.converters import from_df
from friendly_data.dpkg import create_pkg, write_pkg, _resource

from errapids.err import scenario_deltas
from errapids.metrics import ScenarioGroups, pan_eu_cf, pan_eu_prod_share

parser = ArgumentParser(description=__doc__)
parser.add_argument("datadir", help="directory with NetCDF files from Calliope run")
parser.add_argument("outdir", help="Directory where to write the datapackage")
parser.add_argument("--metadata", required=True, help="Config file with metadata")
parser.add_argument(
    "--tech-desc", required=True, help="CSV file with technology descriptions"
)

if __name__ == "__main__":
    opts = parser.parse_args()

    scgrp = ScenarioGroups.from_dir(opts.datadir, "out_scenario*.nc", pretty=False)

    arrs = [scgrp[metric] for metric in scgrp.varnames + scgrp.derived]
    resources = [from_df(arr, basepath=opts.outdir) for arr in arrs]

    deltas = [scenario_deltas(arr) for arr in arrs]
    deltas.extend(
        [
            pan_eu_cf(scgrp["carrier_prod"], scgrp["energy_cap"], grpby)
            for grpby in ("region", "technology")
        ]
    )
    deltas.append(pan_eu_prod_share(scgrp["carrier_prod"]))
    resources.extend(
        [
            from_df(df, basepath=opts.outdir, datapath=f"deltas/{df.columns[0]}.csv")
            for df in deltas
        ]
    )

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
