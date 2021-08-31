#!/usr/bin/env python
from argparse import ArgumentParser
from itertools import product
from pathlib import Path
import sys

import holoviews as hv
import pandas as pd

from errapids.err import scenario_slice, sorted_join
from errapids.io import HDF5Reader
from errapids.viz import baseline_scatter, scenario_heatmap

hv.extension("bokeh")

ix = pd.IndexSlice
parser = ArgumentParser()
parser.add_argument("-i", "--input", help="HDF5 file with dataframes")
parser.add_argument("-o", "--output", help="plot directory")

if __name__ == "__main__":
    opts = parser.parse_args()

    _input = Path(opts.input)
    if not _input.is_file():
        sys.exit(f"{_input!r} is not a file")

    plotdir = Path(opts.output)
    if not plotdir.is_dir():
        sys.exit(f"{plotdir!r} is not a directory")

    reader = HDF5Reader(_input)
    con = reader["carrier_con"].droplevel("carrier")
    prod = reader["carrier_prod"].droplevel("carrier")

    plots = hv.Layout(baseline_scatter(sorted_join(con, prod)))
    hv.save(plots, f"{plotdir}/demand_production_scatter.html")

    countries = "GBR DEU ESP FRA NOR".split()

    for arr, scenario, trans in product(
        [con, prod], "cost demand".split(), [False, True]
    ):
        var = scenario_slice(arr, scenario, countries, trans)

        if scenario == "demand":
            _scenarios = ["heating", "EV"]
        elif scenario == "cost":
            _scenarios = ["PV", "battery"]
        else:
            raise ValueError(f"{scenario}: unsupported scenario")

        if trans:
            if "con" in arr.name.split("_")[-1]:
                tag = "_export"
            elif "prod" in arr.name.split("_")[-1]:
                tag = "_import"
            else:
                raise RuntimeError("shouldn't be here")
        else:
            tag = ""

        fname = f"{opts.output}/{arr.name}_{scenario}{tag}.png"
        print(f"Writing {fname}")
        grid = scenario_heatmap(var, "region", *_scenarios, write=fname)
        grid.fig.suptitle(f"{arr.name} - {scenario}{tag.replace('_', ' - ')}", y=0.999)
        grid.savefig(fname)

        for c in countries:
            _fname = fname.replace(".png", f"_{c}.csv")
            print(f"Writing {_fname}")
            var.xs(c, level="region").unstack(0).to_csv(_fname)
