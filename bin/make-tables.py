#!/usr/bin/env python
from argparse import ArgumentParser
from errapids.err import sorted_join
from pathlib import Path
import sys

import pandas as pd

from errapids.io import HDF5Reader

parser = ArgumentParser()
parser.add_argument("-i", "--input")
parser.add_argument("-m", "--mode", choices=["daily", "annual"], default="annual")


if __name__ == "__main__":
    opts = parser.parse_args()

    if not Path(opts.input).is_file():
        sys.exit(f"{opts.input}: not a file")

    reader = HDF5Reader(opts.input)
    con, prod = [
        reader.ts(name).droplevel("carrier").sum(axis=1).rename(name)
        if opts.mode == "annual"
        else reader.metric(name).droplevel("carrier")
        for name in ("carrier_con", "carrier_prod")
    ]
    df = sorted_join(con, prod).abs() * 0.1  # in TWh
    df = df.assign(prod_over_demand=df["production"] / df["demand"])
    europe = pd.DataFrame(
        {
            "demand": df["demand"].sum(),
            "production": df["production"].sum(),
        },
        index=["Europe"],
    )
    df = pd.concat([europe, df])
    print("In TWh\n", df.to_string(float_format="%.3f"))
