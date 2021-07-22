#!/usr/bin/env python

from argparse import ArgumentParser

from errapids.err import scenario_deltas
from errapids.metrics import ScenarioGroups


parser = ArgumentParser()
parser.add_argument("-o", "--output")
parser.add_argument("-i", "--input")

if __name__ == "__main__":
    opts = parser.parse_args()

    scgrp = ScenarioGroups.from_dir(opts.input, "out_scenario*.nc", pretty=False)
    arrs = [scgrp[metric] for metric in scgrp.varnames + scgrp.derived]

    for arr in arrs:
        arr.to_hdf(opts.output, f"metrics/{arr.name}")
        delta = scenario_deltas(arr)
        delta.to_hdf(opts.output, f"deltas/{arr.name}")
