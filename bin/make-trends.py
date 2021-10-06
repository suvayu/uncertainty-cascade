#!/usr/bin/env python
from argparse import ArgumentParser
from pathlib import Path
import sys

import holoviews as hv

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
    mgr = trendmanager(con, prod, within=countries)

    for lvl in ("EV", "heating"):
        mgr.write(lvl, plotdir)
