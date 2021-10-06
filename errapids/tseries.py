from typing import List, Union, cast

import numpy as np
import pandas as pd

from errapids.err import DFSeries_t, notrans, _prep_agg, ensure_pve, sum_

ix = pd.IndexSlice


def connections(df: pd.DataFrame, region: str) -> List[str]:
    """Return sorted connections for import/export time series

    It's sorted in descending order.

    """
    df_io = notrans(df, invert=False)  # import/export
    if (df_io < 0).any().any():
        df_io = -df_io
    # it's expected that only a fixed scenario combination will be passed
    lvls = df_io.index[0][:4]
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


def smooth(df: DFSeries_t, days: Union[int, float], idx=None) -> DFSeries_t:
    """Time series has 3 hour resolution"""
    if idx:
        return df.loc[idx].rolling(int(8 * days)).mean()
    else:
        return df.rolling(int(8 * days)).mean()


def energy(df: pd.DataFrame, idx, trans: bool) -> pd.DataFrame:
    """Return energy demand/production or import/export time series"""
    df = ensure_pve(df)
    df = sum_(df.loc[idx], "technology", "prod", trans)
    return df


def daily_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Daily mean & avg of time-series"""
    return df.T.groupby(df.columns.date).agg([np.mean, np.var]).T
