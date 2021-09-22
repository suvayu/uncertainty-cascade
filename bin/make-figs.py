#!/usr/bin/env python
from argparse import ArgumentParser
from itertools import product
from pathlib import Path
import sys

import holoviews as hv
import matplotlib.pyplot as plt
import pandas as pd

from errapids.err import scenario_slice
from errapids.io import HDF5Reader
from errapids.metrics import qual_name
from errapids.viz import scenario_heatmap

hv.extension("matplotlib")
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

    countries = "GBR DEU ESP ITA FRA PRT DNK NOR".split()

    reader = HDF5Reader(_input)
    plots = list(
        product(["carrier_con", "carrier_prod"], "cost demand".split(), [False, True])
    )
    plots.extend(
        [
            ("resource_area", "demand", False),
            ("energy_cap", "demand", False),
            ("energy_cap", "cost", False),
        ]
    )

    for metric, scenario, trans in plots:
        if "carrier_con" == metric:
            arr = -reader[metric]
        else:
            arr = reader[metric]
        if "carrier" in arr.index.names:
            arr = arr.droplevel("carrier")
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

        fname = f"{opts.output}/{arr.name}_{scenario}{tag}"
        title = f"{qual_name(arr.name)} - {scenario}{tag.replace('_', ' - ')}"
        print(f"Writing {fname}*.{{png,csv}}")

        grid = scenario_heatmap(var.loc[ix[:, :, countries[:4]]], "region", *_scenarios)
        grid.fig.suptitle(title, y=0.999)
        grid.savefig(f"{fname}-1.png")

        grid = scenario_heatmap(var.loc[ix[:, :, countries[4:]]], "region", *_scenarios)
        grid.fig.suptitle(title, y=0.999)
        grid.savefig(f"{fname}-2.png")
        plt.close(grid.fig)
