#!/usr/bin/env python
"""Generate Calliope overrides for each time series in dataset

"""
from argparse import ArgumentParser

from friendly_data.io import dwim_file

from errapids.ons import get_scenarios, merge_dicts

parser = ArgumentParser()
parser.add_argument("demand", help="Directory with demand profiles for each scenario")
parser.add_argument(
    "--configs",
    nargs="+",
    help="YAML Calliope config files defining locations and technologies",
)
parser.add_argument("--to-year", type=int, default=2015, help="Time series year")
parser.add_argument(
    "--output", help="YAML Calliope config defining overrides and scenarios"
)


if __name__ == "__main__":
    opts = parser.parse_args()
    config = merge_dicts([dwim_file(f) for f in opts.configs])
    scenarios = get_scenarios(config, opts.demand, opts.output, opts.to_year)
    dwim_file(opts.output, scenarios)
