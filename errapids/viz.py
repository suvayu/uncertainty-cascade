from collections import defaultdict
from itertools import product, chain
from typing import List, Tuple, cast

import numpy as np
import pandas as pd
from pycountry import countries
from tqdm import tqdm

import holoviews as hv
from holoviews import opts
import seaborn as sns

from errapids.err import add_groups, sum_
from errapids.err import aggregate
from errapids.err import rgroups
from errapids.err import scenario_deltas
from errapids.err import sort_by_col
from errapids.err import baselines, demand_lvls
from errapids.io import HDF5Reader, _path_t
from errapids.metrics import qual_name, ScenarioGroups, pan_eu_cf, pan_eu_prod_share
from errapids.tseries import connections, daily_summary, energy, smooth

ix = pd.IndexSlice


def new_title(plot, append: str):
    """Append to the title of a Holoviews plot"""
    return plot.opts.get().kwargs["title"] + f" {append}"


def facets1(df: pd.DataFrame, facet: str):
    opts = {
        "ylabel": qual_name(df.columns[0]),
        "width": 400,
        "height": 300,
        "tools": ["hover"],
    }
    if facet == "region":
        _title = lambda k: countries.lookup(k).name
    else:
        _title = lambda k: k

    plots = []
    if not isinstance(df.index, pd.MultiIndex):
        assert df.index.name == "scenario"
        err = hv.Spread(df, vdims=list(df.columns), kdims=["scenario"])
        ref = hv.HLine(df.iloc[0, 0])
        plots.append((err * ref).opts(**opts))
    else:
        for key in df.index.levels[df.index.names.index(facet)]:
            _df = df.query(f"{facet} == '{key}'")
            if _df.empty:
                continue
            err = hv.Spread(_df, vdims=list(df.columns), kdims=["scenario"])
            ref = hv.HLine(_df.query("scenario == 'all'").iloc[0, 0])
            plots.append((err * ref).opts(**opts, title=_title(key)))
    plots.append(hv.Table(df.reset_index()))
    return plots


def facets2(df: pd.DataFrame, facets: List[str]):
    assert len(facets) == 2

    opts = {
        "ylabel": qual_name(df.columns[0]),
        "width": 400,
        "height": 300,
        "tools": ["hover"],
    }
    if "region" in facets:
        _title = lambda k: countries.lookup(k).name
    else:
        _title = lambda k: k

    plots = []
    for region, tech_grp in (
        df.index.droplevel(df.index.names.difference(facets)).unique().to_flat_index()
    ):
        _df = df.query(f"{facets[0]} == '{region}' & {facets[1]} == '{tech_grp}'")
        err = hv.Spread(_df, vdims=list(df.columns), kdims=["scenario"])
        ref = hv.HLine(_df.query("scenario == 'all'").iloc[0, 0])
        plots.append((err * ref).opts(**opts, title=f"{_title(region)} - {tech_grp}"))
    plots.append(hv.Table(df.reset_index()))
    return plots


def get_ylimits(df: pd.DataFrame) -> Tuple[float, float]:
    name = df.columns[0]
    space = 1.7
    rmax = space * df[[name, "errhi"]].sum(axis=1).max()
    rmin = space * df[[name, "errlo"]].diff(periods=-1, axis=1)[name].min()
    if rmin < 0:
        rmax = 0.05
    else:
        rmin = -0.05
    return rmin, rmax


def barchart(df: pd.DataFrame, sumover: str):
    name = df.columns[0]
    opts = {
        "ylabel": qual_name(name),
        "width": 400,
        "height": 300,
        "stacked": True,
        "tools": ["hover"],
        "title": name,
    }

    if not sumover:
        return [
            hv.Bars(df.iloc[:, 0], vdims=[name], kdims=list(df.index.names[:-1])).opts(
                **{**opts, "stacked": False, "ylim": get_ylimits(df)}
            )
        ]

    # order matters
    kdims = ["region", "technology"]
    if 0 == kdims.index(sumover):  # cycle order
        kdims.reverse()

    if sumover == "technology":
        plots = []
        for grp in df.index.levels[df.index.names.index(f"{sumover}_grp")]:
            _df = df.xs((grp, "all"), level=[f"{sumover}_grp", "scenario"])
            plots.append(
                hv.Bars(
                    _df,
                    vdims=[name],
                    kdims=kdims,
                ).opts(**{**opts, "ylim": get_ylimits(_df), "title": f"{name} - {grp}"})
            )
        return plots
    else:
        _df = df.xs("all", level="scenario")
        return [
            hv.Bars(_df, vdims=[name], kdims=kdims,).opts(
                **opts,
                ylim=get_ylimits(_df),
            )
        ]


class plotmanager:
    """Manage and render the plots

    Reads and processes the data on instantiation.  The plots are generated by
    calling :meth:`plot`.  All plots can be written to a directory with
    :meth:`write`.

    """

    @classmethod
    def from_netcdf(cls, datadir: str, glob: str):
        return cls(ScenarioGroups.from_dir(datadir, glob, pretty=False))

    @classmethod
    def from_hdf5(cls, h5path: str):
        return cls(HDF5Reader(h5path))

    def __init__(self, data):
        self._data = data
        self._arrays = {}
        self._trans = {}
        for name in self._data.metrics:
            if any(
                map(
                    lambda i: i in name,
                    ["cost_var", "resource_cap", "resource_con", *self._data.derived],
                )
            ):
                continue
            if name == "carrier_con":
                df = -self._data[name]
            else:
                df = self._data[name]
            self._arrays[name] = scenario_deltas(df, False)
            if "total" in name or "system" in name:
                # no facet over region => no need for region or technology groups
                continue
            if "technology" in df.index.names and list(
                filter(
                    lambda i: i.startswith("ac_trans"),
                    df.index.levels[df.index.names.index("technology")],
                )
            ):
                self._trans[name] = scenario_deltas(df, True)
            self._arrays[name] = (
                self._arrays[name]
                .pipe(add_groups, "technology")
                .pipe(add_groups, "region")
            )

    @property
    def metrics(self):
        return list(self._arrays) + self._data.derived

    @property
    def regions(self) -> List[str]:
        return list(rgroups)

    @classmethod
    def facet(cls, sumover: str) -> str:
        if sumover:
            assert sumover in ("region", "technology")
            return "technology" if sumover == "region" else "region"
        else:
            return "technology"

    def agg_capacity_factor(self, sumover: str):
        df = pan_eu_cf(self._data["carrier_prod"], self._data["energy_cap"], sumover)
        return facets1(df, self.facet(sumover))

    def agg_carrier_prod_share(self):
        df = pan_eu_prod_share(self._data["carrier_prod"])
        return facets1(df, "technology")

    def regionwise_bands(self, scenarios, transmission: bool = False):
        alphas = {"all": 1, "demand": 1, "cost": 1}
        bands = []
        dfs = self._trans if transmission else self._arrays
        for metric, df in dfs.items():
            if "system" in metric or "total" in metric:
                continue
            if transmission:
                if metric not in ("carrier_con", "carrier_prod"):
                    continue
                _df = aggregate(df, "technology", trans=True).loc[ix[:, scenarios], :]
                if _df.empty:
                    continue
            else:
                tgrp = "storage" if "storage" in metric else "prod"
                _df = (
                    aggregate(df, "technology")
                    .droplevel(["region_grp"])
                    .xs(tgrp, level="technology_grp")
                    .loc[ix[:, scenarios], :]
                )
            _df = sort_by_col(_df, scenarios[0])
            plots = {
                lvl: hv.Spread(
                    _df.xs(lvl, level="scenario").head(10),
                    vdims=list(_df.columns),
                    kdims="region",
                ).opts(tools=["hover"])
                for lvl in _df.index.levels[-1]
            }
            # fake a line by setting error to zero
            _noerr = _df.xs(scenarios[0], level="scenario").head(10).assign(errlo=0)
            # the keys are ordered alphabetically, so choose something that comes later
            plots["reference"] = hv.Spread(
                _noerr, vdims=list(_df.columns[:2]), kdims="region"
            ).opts(line_color="black", fill_color="black", fill_alpha=1, line_width=2)
            ylabel = qual_name(_df.columns[0], trans=transmission)
            bands.append(
                hv.NdOverlay(plots, kdims="scenario group").opts(
                    ylabel=ylabel, width=500, height=300
                )
            )
        return bands

    def agg(self, metric: str, sumover: str, region_grp: str):
        if metric in self._data.derived:
            raise ValueError(f"{metric}: derived metric, cannot draw aggregated plot")

        if "total" in metric or "systemwide" in metric:
            df = self._arrays[metric]
        else:
            df = self._arrays[metric].xs(region_grp, level="region_grp")
        df_agg = aggregate(df, sumover)

        if "total" in metric:
            plots = []
        else:
            plots = barchart(df, sumover)

        if self.facet(sumover) == "region":
            plots.extend(facets2(df_agg, ["region", "technology_grp"]))
        else:
            plots.extend(facets1(df_agg, self.facet(sumover)))
        return plots

    def render(self, plots: List, ncols: int = 3, **kwopts):
        return (
            hv.Layout(plots)
            .opts(shared_axes=False, toolbar="right", **kwopts)
            .cols(ncols)
        )

    def write(self, plotdir: str):
        """Write all plots to a directory

        Parameters
        ----------
        plotdir : str
            Plot directory

        """
        pbar1 = tqdm(self.metrics)
        for metric in pbar1:
            pbar1.set_description(f"{metric}")
            if "total" in metric or "systemwide" in metric:
                plot = self.render(self.agg(metric, "", ""))
                hv.save(plot, f"{plotdir}/{metric}.html")
            elif metric in self._data.derived:
                if metric == "capacity_factor":
                    for sumover in ("region", "technology"):
                        plot = self.render(self.agg_capacity_factor(sumover))
                        hv.save(plot, f"{plotdir}/{metric}_{sumover}.html")
                elif metric == "carrier_prod_share":
                    plot = self.render(self.agg_carrier_prod_share())
                    hv.save(plot, f"{plotdir}/{metric}.html")
                else:
                    RuntimeError("don't know how it got here")
            else:
                pbar2 = tqdm(rgroups)
                for lvl, grp in product(("region", "technology"), pbar2):
                    pbar2.set_description(f"{grp=}")
                    plot = self.render(self.agg(metric, lvl, grp))
                    hv.save(plot, f"{plotdir}/{metric}_{grp}_{lvl}.html")

        for _scenarios in (
            ["heating", "EV"],
            ["PV", "battery"],
            ["demand", "cost", "all"],
        ):
            tag = "_".join(_scenarios)
            delta_plots = self.regionwise_bands(_scenarios)
            hv.save(
                self.render(delta_plots, ncols=2),
                f"{plotdir}/scenario_group_delta_{tag}.html",
            )
            delta_plots = self.regionwise_bands(_scenarios, transmission=True)
            hv.save(
                self.render(delta_plots, ncols=2),
                f"{plotdir}/scenario_group_delta_{tag}_transmission.html",
            )


def scenario_heatmap(
    arr: pd.Series, facetx, scenariox, scenarioy, title: str = "", write: str = ""
):
    name = arr.name
    df = arr.reset_index(facetx)
    grid = sns.FacetGrid(df, col=facetx)
    grid.map_dataframe(
        lambda *args, **kwargs: sns.heatmap(
            kwargs.pop("data")[name].unstack(scenariox),
        ),
        scenariox,
        scenarioy,
    )
    if title:
        grid.fig.suptitle(title)
    if write:
        grid.savefig(write)
    return grid


def baseline_scatter(df: pd.DataFrame, nregions: int = 10):
    df = df.head(nregions)
    dims = [
        {
            "vdims": ["production", "region", "import", "export"],
            "kdims": "demand",
        },
        {
            "vdims": ["import", "region", "production", "export"],
            "kdims": "demand",
        },
        {
            "vdims": ["export", "import", "region", "demand"],
            "kdims": "production",
        },
    ]
    scatter_plots = [
        hv.Scatter(df.reset_index(), **_dim).opts(tools=["hover"], width=400)
        for _dim in dims
    ]
    return scatter_plots


class trendmanager:
    days = 7  # days to smooth

    def __init__(
        self, con: pd.DataFrame, prod: pd.DataFrame, within: List[str], lvls=None
    ):
        self.con = cast(pd.DataFrame, -con)
        self.prod = prod
        self.within = within
        if lvls:
            self.lvls = lvls
        else:
            self.lvls = [l for l in baselines.values() if not isinstance(l, list)]

    @property
    def lvls(self):
        return self._lvls

    @lvls.setter
    def lvls(self, _lvls):
        self._lvls = _lvls
        self._update()

    @property
    def idx(self):
        return ix[(*self.lvls, slice(None))]

    def widx(self, *args):
        return ix[(*self.lvls, *args)]

    def _update(self):
        self.demand = energy(self.con, self.idx, trans=False)
        self.generated = energy(self.prod, self.idx, trans=False)

    def legend(self, export: bool, split: bool = False):
        if split:
            return "exported to" if export else "imported from"
        else:
            return "exported" if export else "imported"

    def transmission(self, export: bool, raw: bool):
        df = self.con if export else self.prod
        if not raw:
            res = energy(df, self.idx, trans=True)
            return res
        return df

    def plot(self, region: str, export: bool):
        """Plot time series for `region`, include export/import"""
        links = connections(self.transmission(export, raw=True).loc[self.idx], region)
        ts = {
            ("demand", region): smooth(
                self.demand, self.days, self.widx(region)
            ).rename("electricity"),
            ("generated", region): smooth(
                self.generated, self.days, self.widx(region)
            ).rename("electricity"),
            (self.legend(export), region): smooth(
                self.transmission(export, raw=False), self.days, self.widx(region)
            ).rename("electricity"),
            **{
                (self.legend(export, split=True), r): smooth(
                    self.transmission(export, raw=True),
                    self.days,
                    self.widx(region, f"ac_transmission:{r}"),
                ).rename("electricity")
                for r in links[:3]
                if r in self.within
            },
        }

        trend = hv.NdOverlay(
            {
                k: hv.Curve(v).opts(ylabel=qual_name(v.name), tools=["hover"])
                for k, v in ts.items()
            },
            kdims=["electricity type", "country"],
        ).opts(
            width=800,
            height=400,
            title=f"{region}: Electricity demand, generation, & {self.legend(export)[:-2]}",
        )
        return trend

    def plot_var(self, region: str):
        eu_regions = list(chain(rgroups, ("", "eu", "EU")))

        def _summary(df):
            if region in rgroups:
                df = df.pipe(sum_, "region", region)
            elif region in ("", "eu", "EU"):
                df = df.pipe(sum_, "region", False)

            df = daily_summary(df)
            if region not in eu_regions:
                df = df.xs(region, level="region")

            df = df.T.rolling(self.days).mean().reset_index()
            df.columns = ["day", "electricity", "variance"]
            return df

        ts = {k: _summary(getattr(self, k)) for k in ("demand", "generated")}
        trend = hv.NdOverlay(
            {
                k: hv.Spread(v).opts(ylabel=qual_name(v.columns[1]))
                * hv.Curve(v[["day", "electricity"]]).opts(tools=["hover"])
                for k, v in ts.items()
            },
            kdims="electricity",
        ).opts(
            width=800,
            height=400,
            title=f"{region}: Electricity demand, & generation",
        )
        # NOTE: can't combine kwd opts with Option objects
        col_cy = hv.Cycle("Colorblind")
        return trend.opts(opts.Curve(color=col_cy), opts.Spread(color=col_cy))

    def write(self, lvl: str, plotdir: _path_t):
        plots1 = defaultdict(list)
        plots2 = defaultdict(list)
        pbar = tqdm(demand_lvls)
        for val in pbar:
            pbar.set_description(f"{lvl}={val}")
            lvls = [
                val if l == lvl else v
                for l, v in baselines.items()
                if not isinstance(v, list)
            ]
            self.lvls = lvls
            for region, export in product(self.within, (True, False)):
                key = (region, export)
                plot = self.plot(region, export=export)
                plots1[key].append(plot.opts(title=new_title(plot, f"({lvl}={val})")))
                if export:
                    plot = self.plot_var(region)
                    plots2[region].append(
                        plot.opts(title=new_title(plot, f"({lvl}={val})"))
                    )

            for region in list(rgroups) + ["EU"]:
                plot = self.plot_var(region)
                plots2[region].append(
                    plot.opts(title=new_title(plot, f"({lvl}={val})"))
                )

        pbar = tqdm(plots1.items())
        for (region, export), plot in pbar:
            pbar.set_description(f"Writing file:{region=},{export=}")
            hv.save(
                hv.Layout(plot).cols(1),
                f"{plotdir}/electricity_{region}_{self.legend(export)}_{lvl}.html",
            )

        pbar = tqdm(plots2.items())
        for region, plot in pbar:
            pbar.set_description(f"Writing file:{region=}")
            hv.save(hv.Layout(plot).cols(1), f"{plotdir}/elec_var_{region}_{lvl}.html")
