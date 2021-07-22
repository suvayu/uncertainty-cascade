from functools import partial
from typing import Dict, List, Sequence, Tuple, TypeVar, Union

import numpy as np
import pandas as pd

# region groups
rgroups = {
    "nordic": ["DNK", "FIN", "NOR", "SWE"],  # scandinavia
    "baltic": ["EST", "LTU", "LVA"],  # east of scandinavia
    "west": ["DEU", "FRA", "NLD", "LUX", "POL"],  # west
    "med": ["ESP", "ITA", "GRC", "PRT"],  # mediterranean
    "isles": ["GBR", "IRL"],  # british isles
    "balkan": ["MKD", "ROU", "SRB", "SVN", "HRV", "SVK", "HUN", "CZE"],  # balkan
}
# technology groups
tgroups = {
    "prod": [
        "biofuel",
        "hydrogen",
        "open_field_pv",
        "roof_mounted_pv",
        "wind_offshore",
        "wind_onshore_competing",
        "wind_onshore_monopoly",
    ],
    "storage": ["battery", "pumped_hydro"],
    "hydro": ["hydro_reservoir", "hydro_run_of_river"],
}
# scenario baselines
baselines = {
    "heating": "mid",
    "EV": "low",
    "PV": 100,
    "battery": 100,
    "demand": ["PV", "battery"],  # order same as in index
    "cost": ["heating", "EV"],
    "all": [],  # no pins
}


def _isgrouped(scenario: str) -> bool:
    """Check if scenario is a meta (grouped) scenario

    Convention: for a regular scenario, the value is the baseline value.  For a
    grouped scenario, it is a list of other scenarios that are pinned to their
    baselines.

    """
    return isinstance(baselines[scenario], list)


def qsum(data: Sequence) -> float:
    """Add in quadrature, typically uncertainties

    .. math::

    \\sqrt{\\sum_{i=1}^{n} x^2_i}

    """
    return np.sqrt(np.square(data).sum())


DFSeries_t = TypeVar("DFSeries_t", pd.DataFrame, pd.Series)


def lvl_filter(
    df: DFSeries_t, lvl: str, token: str, invert: bool = True, reverse: bool = False
) -> DFSeries_t:
    """Filter a dataframe by the values in the index at the specified level

    Parameters
    ----------
    df : Union[pandas.DataFrame, pandas.Series]
        Dataframe/series

    lvl : str
        Level name

    invert : bool (default: True)
        Whether to filter out (default) matches, or select matches

    reverse : bool (default: False)
        Whether to match the start (default) or end of the value

    Returns
    -------
    Union[pandas.DataFrame, pandas.Series]
        The filtered dataframe/series

    """
    if reverse:
        sel = df.index.get_level_values(lvl).str.endswith(token)
    else:
        sel = df.index.get_level_values(lvl).str.startswith(token)
    return df[~sel if invert else sel]


notrans = partial(lvl_filter, lvl="technology", token="ac_transmission")


def marginalise(arr: pd.Series, scenario: str) -> Union[pd.DataFrame, pd.Series]:
    """Marginalise all scenarios except the one specified.

    Pin all scenarios except the one specified to their baselines, and
    calculate the uncertainty based on the remaining scenarios.  The lower and
    upper values are put in columns `errlo` and `errhi`.  The returned
    dataframe has the columns: `<original column>`, `errlo`, and `errhi`.

    Parameters
    ----------
    df : pandas.DataFrame
        Dataframe

    scenario : str
        The scenario name; should be one of the scenarios defined in `baselines`

    Returns
    -------
    NDFrame
        The marginalised dataframe/series

    """
    colname = arr.name
    df = arr.to_frame()
    if scenario not in baselines:
        raise ValueError(f"unknown {scenario=}: must be one of {list(baselines)}")

    # pin other scenarios
    pins = (
        [sc for sc in baselines[scenario]]
        if _isgrouped(scenario)
        else [sc for sc in baselines if sc != scenario and not _isgrouped(sc)]
    )
    if pins:
        query = [f"{sc} == {baselines[sc]!r}" for sc in pins]
        df = df.query(" & ".join(query))
        df.index = df.index.droplevel(pins)

    unpinned = [
        baselines[sc] for sc in baselines if sc not in pins and not _isgrouped(sc)
    ]
    df = df.unstack(list(range(len(unpinned))))
    baseline = df[(colname, *unpinned) if unpinned else colname]
    names = [colname, "errlo", "errhi"]
    if df.ndim > 1:
        deltas = [(df.apply(f, axis=1) - baseline).abs() for f in (np.min, np.max)]
        df = pd.concat([baseline, *deltas], axis=1)
        df.columns = names
        return df
    else:  # series
        deltas = [np.abs(f(df) - baseline) for f in (np.min, np.max)]
        return pd.Series([baseline, *deltas], index=names)


def scenario_deltas(arr: pd.Series, istransmission: bool = False) -> pd.DataFrame:
    """Calculate uncertainties for different scenarios.

    This adds a new level in the index for every scenario.

    Parameters
    ----------
    arr : pandas.Series
        Dataframe

    istransmission : bool (default: False)
        Whether to consider transmission data or not

    Returns
    -------
    pandas.DataFrame

    """
    if "technology" in arr.index.names:
        arr = lvl_filter(arr, "technology", "ac_trans", invert=not istransmission)
    if arr.empty:
        raise ValueError(f"{istransmission=}: returned empty dataframe")
    if "carrier" in arr.index.names:  # redundant for us
        arr.index = arr.index.droplevel("carrier")

    # FIXME: instead of iterating, could do it by generating the right indices
    # in marginalise
    arrays = []
    for scenario in baselines:
        _df = marginalise(arr, scenario)
        if isinstance(_df, pd.DataFrame):
            null = _df.apply(lambda row: np.isclose(row, 0).all(), axis=1)
            _df = (
                _df[~null].assign(scenario=scenario).set_index("scenario", append=True)
            )
        else:  # series
            _df = pd.DataFrame([_df], index=pd.Index([scenario], name="scenario"))
        arrays.append(_df)
    df = pd.concat(arrays, axis=0)
    if isinstance(df.index, pd.MultiIndex):
        return df.reindex(index=list(baselines), level="scenario")
    else:
        return df.reindex(index=list(baselines))
