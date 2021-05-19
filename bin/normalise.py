#!/usr/bin/env python
"""Normalise dataset by setting country names to 3-letter ISO codes

"""

from argparse import ArgumentParser
from pathlib import Path

from errapids.io import destinee2calliope_csv


parser = ArgumentParser(description=__doc__)
parser.add_argument("indatadir")
parser.add_argument("outdatadir")

if __name__ == "__main__":
    opts = parser.parse_args()
    outdatadir = Path(opts.outdatadir)
    for inpath in Path(opts.indatadir).glob("*.csv"):
        destinee2calliope_csv(inpath, outdatadir / inpath.name)
