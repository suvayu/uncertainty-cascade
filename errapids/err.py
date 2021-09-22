from functools import partial
from functools import reduce
from logging import getLogger
from typing import List, Sequence, Tuple, TypeVar, Union

import numpy as np
import pandas as pd

logger = getLogger(__name__)

ix = pd.IndexSlice

# region groups
rgroups = {
    "nordic": ["DNK", "FIN", "NOR", "SWE"],
    "baltic": ["EST", "LTU", "LVA"],
    "west": ["DEU", "FRA", "NLD", "BEL", "LUX", "POL", "CHE", "AUT"],
    "med": ["ESP", "ITA", "GRC", "PRT"],  # mediterranean
    "isles": ["GBR", "IRL"],  # british isles
    "balkan": [
        "ALB",
        "BIH",
        "BGR",
        "MKD",
        "ROU",
        "SRB",
        "SVN",
        "HRV",
        "SVK",
        "HUN",
        "CZE",
    ],
}
# technology groups
tgroups = {
    "prod": [
        "demand_elec",
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
    "EV": "mid",
    "PV": 100,
    "battery": 100,
    "demand": ["PV", "battery"],  # order same as in index
    "cost": ["heating", "EV"],
    "all": [],  # no pins
}
demand_lvls = ["low", "mid", "high"]
cost_lvls = [100, 70, 50]
scenario_lvls = {
    "heating": demand_lvls,
    "EV": demand_lvls,
    "PV": cost_lvls,
    "battery": cost_lvls,
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


def add_groups(arr: DFSeries_t, atlvl: str) -> DFSeries_t:
    if atlvl not in ("technology", "region"):
        raise ValueError(f"{atlvl}: unsupported level")

    groupmap = tgroups if atlvl == "technology" else rgroups
    newlvl = f"{atlvl}_grp"

    rmap = {v: k for k, _v in groupmap.items() for v in _v}
    idx = arr.index.to_frame()
    idx.insert(
        arr.index.names.index(atlvl),
        newlvl,
        arr.index.get_level_values(atlvl).map(rmap),
    )
    arr.index = pd.MultiIndex.from_frame(idx)
    return arr


def _prep_agg(
    arr: DFSeries_t, sumover: str, restrict: str = "", trans: bool = False
) -> Tuple[str, DFSeries_t]:
    if isinstance(arr, pd.Series):
        name = arr.name
    else:
        name = arr.columns[0]

    if "technology" in arr.index.names:
        arr = notrans(arr, invert=not trans)

    if (not trans and sumover == "technology") or (
        restrict and sumover in arr.index.names
    ):
        if f"{sumover}_grp" not in arr.index.names:
            arr = add_groups(arr, sumover)

    if restrict and sumover in arr.index.names:
        arr = arr.xs(restrict, level=f"{sumover}_grp")
    elif restrict:
        logger.warning(f"{name}: no {sumover} level in index, {restrict=} ignored")
    else:
        logger.info(f"{name}: no {sumover} level in index")

    return name, arr


def aggregate(
    df: pd.DataFrame, sumover: str, restrict: str = "", trans: bool = False
) -> pd.DataFrame:
    name, df = _prep_agg(df, sumover, restrict, trans)
    grpd = df.groupby(df.index.names.difference([sumover])).agg(
        {name: np.sum, "errlo": qsum, "errhi": qsum}
    )
    if isinstance(grpd.index, pd.MultiIndex):
        return grpd.reindex(index=list(baselines), level="scenario")
    else:
        return grpd.reindex(index=list(baselines))


def aggregate2(
    arr: DFSeries_t, sumover: str, restrict: str = "", trans: bool = False
) -> DFSeries_t:
    """Sum over an index level.  Group the indices appropriately before summation.
    Filter transmission (keep/reject) depending on `trans`

    """
    name, arr = _prep_agg(arr, sumover, restrict, trans)
    grpd = arr.groupby(arr.index.names.difference([sumover]))
    return grpd.sum() if isinstance(arr, pd.Series) else grpd.agg({name: np.sum})


def sum_n_slice(arr: DFSeries_t, idx, trans: bool = False):
    if "carrier" in arr.index.names:
        arr = arr.droplevel("carrier")
    if trans:
        arr = arr.pipe(notrans, invert=False).pipe(aggregate2, "technology")
    else:
        arr = (
            arr.copy()
            .pipe(notrans)
            .pipe(add_groups, "technology")
            .pipe(aggregate2, "technology", "prod")
        )
    return arr.loc[idx]


def sort_by_col(df: pd.DataFrame, scenario: str) -> pd.DataFrame:
    """Sort by the first column for the given `scenario`"""
    is_neg = df.iloc[:, 0].mean() < 0
    df_sorted = (
        df.xs(scenario, level="scenario").iloc[:, 0].sort_values(ascending=is_neg)
    )
    regions = df_sorted.index
    df = df.reindex(index=regions, level="region")
    df.index = df.index.remove_unused_levels()
    return df


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
    if df.empty:
        raise RuntimeError(f"{arr.name}, {istransmission=}: empty dataframe, ~0")
    if isinstance(df.index, pd.MultiIndex):
        return df.reindex(index=list(baselines), level="scenario")
    else:
        return df.reindex(index=list(baselines))


def delta_prop(df: pd.DataFrame, prop: bool = False, sfx: str = "") -> pd.Series:
    """Proportional uncertainty"""
    name = df.columns[0]
    newname = f"{df.columns[0]}_{sfx}" if sfx else name
    hi = df[[name, "errhi"]].sum(axis=1)
    lo = df[[name, "errlo"]].diff(periods=-1, axis=1).iloc[:, 0]
    if prop:
        return hi.sub(lo).div(df[name]).rename(newname)
    else:
        return hi.sub(lo).rename(newname)


def scenario_slice(
    arr: pd.Series, scenario: str, regions: List[str], trans: bool
) -> pd.Series:
    """TODO: support single scenario"""
    if scenario == "demand":
        _slice = ix[demand_lvls, demand_lvls, 100, 100, regions]
        _scenarios = ["heating", "EV"]
        _drop = ["PV", "battery"]
    elif scenario == "cost":
        _slice = ix["mid", "mid", cost_lvls, cost_lvls, regions]
        _scenarios = ["PV", "battery"]
        _drop = ["heating", "EV"]
    else:
        raise ValueError(f"{scenario}: unsupported scenario")

    if trans:
        _df = aggregate2(arr, "technology", trans=True)
    else:
        _df = aggregate2(arr, "technology", "prod")
    var = reduce(
        lambda d, l: d.reindex(scenario_lvls[l], level=l),
        _scenarios,
        _df.loc[_slice].droplevel(_drop),
    )  # fix index ordering
    return var


def sorted_join(
    con: pd.Series, prod: pd.Series, sort_first: bool = True
) -> pd.DataFrame:
    def delta_by_region(df):
        return df.pipe(scenario_deltas).pipe(aggregate2, "technology", "prod")

    def delta_by_region_trans(df):
        return df.pipe(scenario_deltas, istransmission=True).pipe(
            aggregate2, "technology", trans=True
        )

    con1 = delta_by_region(con)
    con2 = delta_by_region_trans(con)  # export
    prod1 = delta_by_region(prod)
    prod2 = delta_by_region_trans(prod)  # import

    regions = (
        sort_by_col(con1 if sort_first else prod1, "all")
        .loc[ix[:, "all"], :]
        .droplevel("scenario")
        .index
    )

    df_joined = (
        pd.concat(
            [
                con1.iloc[:, 0].rename("demand"),
                prod1.iloc[:, 0].rename("production"),
                con2.iloc[:, 0].rename("export"),
                prod2.iloc[:, 0].rename("import"),
            ],
            axis=1,
        )
        .xs("all", level="scenario")
        .reindex(regions)
    )
    return df_joined
