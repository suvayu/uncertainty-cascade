#!/usr/bin/env python
"""Generate Calliope overrides for each time series in dataset

"""
from argparse import ArgumentParser

from friendly_data.io import dwim_file

from errapids.ons import get_scenarios

parser = ArgumentParser()
parser.add_argument("demand", help="Directory with demand profiles for each scenario")
parser.add_argument("--config", help="YAML Calliope config defining locations")
parser.add_argument(
    "--output", help="YAML Calliope config defining overrides and scenarios"
)


if __name__ == "__main__":
    opts = parser.parse_args()
    config = dwim_file(opts.config)
    scenarios = get_scenarios(config, opts.demand, opts.output)
    dwim_file(opts.output, scenarios)
