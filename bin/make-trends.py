#!/usr/bin/env python
from argparse import ArgumentParser
from collections import defaultdict
from itertools import product
from pathlib import Path
import sys

import holoviews as hv
from tqdm import tqdm

from errapids.err import baselines, demand_lvls
from errapids.io import HDF5Reader
from errapids.viz import trendmanager

hv.extension("bokeh")
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
    con = reader.ts("carrier_con").droplevel("carrier")
    prod = reader.ts("carrier_prod").droplevel("carrier")
    countries = "GBR DEU ESP ITA FRA PRT DNK NOR".split()

    pbar = tqdm(demand_lvls)
    plots = defaultdict(list)
    mgr = trendmanager(con, prod, within=countries)
    for val in pbar:
        pbar.set_description(f"EV={val}")
        lvls = [
            val if l == "EV" else v
            for l, v in baselines.items()
            if not isinstance(v, list)
        ]
        mgr.lvls = lvls
        for region, export in product(countries, (True, False)):
            key = (region, export)
            plot = mgr.plot(region, export=export)
            plots[key].append(
                plot.opts(title=plot.opts.get().kwargs["title"] + f"(EV={val})")
            )
    for (region, export), plot in plots.items():
        hv.save(
            hv.Layout(plot).cols(1),
            f"{plotdir}/electricity_{region}_{mgr.legend(export)}_EV.html",
        )
