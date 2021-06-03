#!/usr/bin/env python
"""Normalise dataset by setting country names to 3-letter ISO codes

"""

from argparse import ArgumentParser
from pathlib import Path

from errapids.io import destinee2calliope_csv


parser = ArgumentParser(description=__doc__)
parser.add_argument("indatadir")
parser.add_argument("outdatadir")
parser.add_argument(
    "--to-year", type=int, default=2015, help="Target year for time series"
)

if __name__ == "__main__":
    opts = parser.parse_args()
    outdatadir = Path(opts.outdatadir)
    print(f"Time series target year: {opts.to_year}")
    outdatadir.mkdir(parents=True, exist_ok=True)
    for inpath in Path(opts.indatadir).glob("*.csv"):
        destinee2calliope_csv(inpath, outdatadir / inpath.name, opts.to_year)
