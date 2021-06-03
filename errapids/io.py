"""Utilites to preprocess CSV files from DESTinEE for Calliope"""

from datetime import datetime
from pathlib import Path
from typing import Iterable, Tuple, Union

import pandas as pd
from pycountry import countries
import xarray as xr

_path_t = Union[str, Path]
_scenario_t = Tuple[str, str]


class alias_dict(dict):
    """Dictionary with aliases and noops

    NOTE: keyword arguments to initialise the dictionary isn't supported

    """

    def __init__(self, arg, *, aliases={}, noops=[]):
        self._aliases = aliases
        self._noops = noops
        super().__init__(arg)

    def __missing__(self, key):
        if key in self._aliases:
            return self.__getitem__(self._aliases[key])
        elif key in self._noops:
            return key
        else:
            raise KeyError(key)


country_map = alias_dict(
    ((c.name, c.alpha_3) for c in countries),
    # NOTE: alias non-standard country names used by DESTINEE
    aliases={
        "UK": "United Kingdom",
        "Moldova": "Moldova, Republic of",
        "Macedonia": "North Macedonia",
        "Bosnia Herz.": "Bosnia and Herzegovina",
        "Czech Rep.": "Czechia",
    },
    # fallthrough non-country column
    noops=["timestep"],
)


def read_csv_to_df(fpath: _path_t, to_yr: int, from_yr: int = 2050) -> pd.DataFrame:
    df = pd.read_csv(
        fpath,
        header=0,
        infer_datetime_format=True,
        parse_dates=True,
        index_col=0,
        dayfirst=True,
    )
    df.columns = df.columns.map(country_map).astype("category")
    # NOTE: reindex to match capacity factor timestep
    shift = datetime(from_yr, 1, 1) - datetime(to_yr, 1, 1)
    df.index = df.index.shift(-shift.days, freq="D")
    # NOTE: demand is -ve in calliope, and default units are 1GW in destinee as
    # opposed to 100GW in euro calliope
    return df * -1e-2


def destinee2calliope_csv(inpath: _path_t, outpath: _path_t, to_yr: int):
    # NOTE: match datetime format with capacity factor timeseries
    df = read_csv_to_df(inpath, to_yr=to_yr)
    df.to_csv(outpath, date_format="%Y-%m-%d %H:%M")  # , float_format="{0:.5f}".format)


_flevel_aliases = {
    "high": ["high", "hi"],
    "mid": ["mid", "medium"],
    "low": ["low", "lo"],
}


def decode_fname(fname: str) -> _scenario_t:
    tokens = fname.lower().split("_")

    def remap(token: str) -> str:
        key = [k for k, v in _flevel_aliases.items() if token in v]
        assert key
        return key[0]

    # building heating, charging profile
    heating = remap(tokens[0])
    charging = remap(tokens[2])
    # return f"{heating}_{charging}"
    return heating, charging
