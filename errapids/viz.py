from typing import Dict, Sequence, Tuple, TypeVar, Union

import numpy as np
import pandas as pd

import holoviews as hv

from errapids.metrics import metric_as_dfs

# region groups
rgroups = {
    "nordic": ["DNK", "FIN", "NOR", "SWE"],  # scandinavia
    "baltic": ["EST", "LTU", "LVA"],  # east of scandinavia
    "poland": ["POL"],  # poland
    "west": ["DEU", "FRA", "NLD", "LUX"],  # west
    "med": ["ESP", "ITA", "GRC", "PRT"],  # mediterranean
    "isles": ["GBR", "IRL"],  # british isles
    "balkan": ["MKD", "ROU", "SRB", "SVN", "HRV"],  # balkan with coast line
    "landlocked": ["SVK", "HUN", "CZE"],  # land locked
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


def marginalise(df: pd.DataFrame, scenario: str) -> Union[pd.DataFrame, pd.Series]:
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
    colname = df.columns[0]
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


def scenario_deltas(df: pd.DataFrame, istransmission: bool = False) -> pd.DataFrame:
    """Calculate uncertainties for different scenarios.

    This adds a new level in the index for every scenario.

    Parameters
    ----------
    df : pandas.DataFrame
        Dataframe

    istransmission : bool (default: False)
        Whether to consider transmission data or not

    Returns
    -------
    pandas.DataFrame

    """
    if "technology" in df.index.names:
        df = lvl_filter(df, "technology", "ac_transmission", invert=not istransmission)
    if df.empty:
        raise ValueError(f"{istransmission=}: returned empty dataframe")
    if "carrier" in df.index.names:  # redundant for us
        df.index = df.index.droplevel("carrier")

    # FIXME: instead of iterating, could do it by generating the right indices
    # in marginalise
    dfs = []
    for scenario in baselines:
        _df = marginalise(df, scenario)
        if isinstance(_df, pd.DataFrame):
            null = _df.apply(lambda row: np.isclose(row, 0).all(), axis=1)
            _df = (
                _df[~null].assign(scenario=scenario).set_index("scenario", append=True)
            )
        else:  # series
            _df = pd.DataFrame([_df], index=pd.Index([scenario], name="scenario"))
        dfs.append(_df)
    return pd.concat(dfs, axis=0)


def elements(df: pd.DataFrame, region: str, groupby: str) -> Dict[str, Tuple]:
    """Create plot elements for the subset ``region``, faceted by ``groupby``

    Parameters
    ----------
    df : pandas.DataFrame
        Dataframe

    region : str
        Create plots for this region subset; regions are defined in `rgroups`

    groupby : str ("region" or "technology")
        Facet the plots by this index level, the other level is summed
        (stacked); one of ``region`` or ``technology``.

    Returns
    -------
    Dict[str, Tuple[hvplot.Bars, hvplot.ErrorBars, pandas.DataFrame, pandas.DataFrame]]
        Dictionary of "plots".  Each "plot" is one of the facets, and is a
        tuple of ``hvplot.Bars``, ``hvplot.ErrorBars``, and the two
        corresponding dataframes they were made from.

    """
    name = df.columns[0]

    if not isinstance(df.index, pd.MultiIndex):
        return {
            name: (
                hv.Bars(df, vdims=[name], kdims=["scenario"]).opts(alpha=0.7),
                hv.ErrorBars(
                    df, vdims=[name, "errlo", "errhi"], kdims=["scenario"]
                ).opts(line_width=1.5),
                df,
                df,
            )
        }

    if groupby not in df.index.names:
        raise ValueError(f"{name}: {groupby=} not present")

    if "region" in df.index.names:
        grp = rgroups[region]  # noqa, choose region, implicitly used in df.query(..)
        df = df.query("region in @grp")
    else:
        print(f"{name}: no region level in index, {region=} ignored")
    grouped = df.groupby(groupby)

    stacked = list(filter(lambda i: i not in ["scenario", groupby], df.index.names))
    elements = {}
    for key in grouped.groups:
        _df = grouped.get_group(key)
        _stacked = _df.groupby("scenario").agg(
            {name: np.sum, "errlo": qsum, "errhi": qsum}
        )
        # FIXME: deduce from data
        space = 1.7 if groupby == "region" else 1.2
        rmax = space * _stacked[[name, "errhi"]].sum(axis=1).max()
        bars = hv.Bars(_df, vdims=[name], kdims=["scenario", *stacked]).opts(
            stacked=True, alpha=0.7, ylim=(0, rmax), tools=["hover"]
        )
        errs = hv.ErrorBars(
            _stacked, vdims=[name, "errlo", "errhi"], kdims=["scenario"]
        ).opts(line_width=1.5, ylim=(0, rmax), tools=["hover"])
        elements[key] = (bars, errs, _df, _stacked)
    return elements


class plotmanager:
    """Manage and render the plots

    Reads and processes the data on instantiation.  The plots are generated by
    calling :meth:`plot`.  All plots can be written to a directory with
    :meth:`write`.

    """

    def __init__(self, datadir: str, glob: str, istransmission: bool = False):
        self._dfs = {
            df.columns[0]: scenario_deltas(df, istransmission)
            for df in metric_as_dfs(datadir, glob, pretty=False)
        }

    @property
    def metrics(self):
        return list(self._dfs)

    @property
    def regions(self):
        return list(rgroups)

    def plot(self, metric: str, region: str, groupby: str) -> hv.Layout:
        """Render plot

        Parameters
        ----------
        metric : str
            Metric to plot

        region : str
            Region subset to include in plot

        groupby : str
            Index level to facet

        Returns
        -------
        hvplot.Layout
            A 2-column ``hvplot.Layout`` with the different facets of the plot

        """
        plottables = elements(self._dfs[metric], region, groupby)
        figures = []
        for facet, (bar, err, *_) in plottables.items():
            fig = (bar * err).opts(title=facet, width=800, height=600)
            figures.append(fig)
        return hv.Layout(figures).cols(2)

    def write(self, plotdir: str):
        """Write all plots to a directory

        Parameters
        ----------
        plotdir : str
            Plot directory

        """
        for metric in self.metrics:
            if "total" in metric or "systemwide" in metric:
                continue
            for region in rgroups:
                for grouping in ("region", "technology"):
                    plots = self.plot(metric, region, grouping)
                    hv.save(plots, f"{plotdir}/{metric}_{region}_{grouping}.html")

        for metric in self.metrics:
            if "systemwide" in metric:
                grouping = "technology"
            elif "total" in metric:
                grouping = ""
            else:
                continue
            plots = self.plot(metric, "", grouping)
            hv.save(
                plots, f"{plotdir}/{metric}{'_' + grouping if grouping else ''}.html"
            )
