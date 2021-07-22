#!/usr/bin/env python
from argparse import ArgumentParser
from pathlib import Path

import holoviews as hv

from errapids.viz import plotmanager

hv.extension("bokeh")

parser = ArgumentParser()
parser.add_argument("-i", "--input")
parser.add_argument("-o", "--output")

if __name__ == "__main__":
    opts = parser.parse_args()

    _input = Path(opts.input)
    if _input.is_dir():
        mgr = plotmanager.from_netcdf(str(_input), "out_scenario*.nc", pretty=False)
    else:
        mgr = plotmanager.from_hdf5(str(_input))
    mgr.write(opts.output)
