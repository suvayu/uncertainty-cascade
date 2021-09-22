from typing import List

import pandas as pd

from errapids.err import baselines, notrans, _prep_agg

ix = pd.IndexSlice


def connections(df: pd.DataFrame, region: str) -> List[str]:
    """Return sorted connections for import/export time series

    It's sorted in descending order.

    """
    # print(df)
    df_io = notrans(df, invert=False)  # import/export
    if (df_io < 0).any().any():
        df_io = -df_io
    lvls = [l for l in baselines.values() if not isinstance(l, list)]
    regions = (
        df_io.loc[ix[(*lvls, region, slice(None))]]
        .mean(axis=1)
        .sort_values(ascending=False)
        .index.get_level_values("technology")
        .str.split(":", expand=True)
        .to_frame()
        .iloc[:, 1]
        .to_list()
    )
    return regions


def smooth(df: pd.DataFrame, days: int, idx=None) -> pd.DataFrame:
    """Time series has 3 hour resolution"""
    if idx:
        return df.loc[idx].rolling(8 * days).mean()
    else:
        return df.rolling(8 * days).mean()


def energy(df: pd.DataFrame, idx, trans: bool) -> pd.DataFrame:
    """Return energy demand/production or import/export time series"""
    if trans:
        name, df = _prep_agg(df.loc[idx], "technology", trans=trans)
    else:
        name, df = _prep_agg(df.loc[idx], "technology", restrict="prod")

    if (df < 0).any().any():
        df = -df
    return df.groupby("region").sum()
