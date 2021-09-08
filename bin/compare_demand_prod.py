#!/usr/bin/env python
from argparse import ArgumentParser
from pathlib import Path
import sys

import holoviews as hv
import pandas as pd

from errapids.err import sorted_join
from errapids.io import HDF5Reader
from errapids.viz import baseline_scatter

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

    df1 = sorted_join(con, prod)
    plots1 = baseline_scatter(df1)
    plots1.append(hv.Table(df1.reset_index()))
    lo = hv.Layout(plots1).cols(2)
    hv.save(lo, f"{plotdir}/demand_production_scatter_sort_by_demand.html")

    df2 = sorted_join(con, prod, sort_first=False)
    plots2 = baseline_scatter(df2)
    plots2.append(hv.Table(df2.reset_index()))
    lo = hv.Layout(plots2).cols(2)
    hv.save(lo, f"{plotdir}/demand_production_scatter_sort_by_prod.html")
