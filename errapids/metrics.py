from typing import Union

import pandas as pd
import xarray as xr

from errapids.io import _path_t


class Metrics:
    """Parses the Calliope model result and provides summary metrics

    A Calliope model result encodes the factor/categorical variables for some
    of the metrics by concatenating them: location, technology, [carrier].
    Others combination are left as is: e.g. carrier, cost, technology.  This
    class parses the concatenated factor/categorical variables, as well as
    resolves their order into one definite order: cost, location, technology,
    carrier.

    Cost is always "summed" over as usually there is only one type of cost
    (monetary).  There can be other kinds of costs, like emissions, in which
    case this class has to be updated.

    If a factor/level is absent, it is simply skipped in the summary.

    >>> metrics = Metrics.from_netcdf("path/to/model_result.nc")
    >>> metrics["energy_cap"]
    # a dataframe

    """

    @classmethod
    def from_netcdf(cls, fpath: _path_t) -> "Metrics":
        """Read the saved model result from a NetCDF file"""
        return cls(xr.open_dataset(fpath))

    def __init__(self, dst: xr.Dataset):
        self._dst = dst.filter_by_attrs(is_result=1)

    def __repr__(self) -> str:
        return repr(self._dst)

    def __getitem__(self, metric: str) -> pd.Series:
        """Access a metric from the model result"""
        darr = self._dst[metric]
        if "costs" in darr.dims:  # costs: redundant, usually only monetary
            darr = darr.sel(costs="monetary")
        arr = darr.to_pandas()  # series or dataframe (when timeseries)

        if "carriers" in darr.dims and len(darr.dims) > 1:  # when not concatenated
            arr = arr.stack()  # dataframe -> series

        if arr.ndim == 2:  # timesteps: aggregated
            arr = arr.mean(axis=1)  # dataframe -> series
        assert arr.ndim == 1

        # factors of interest: location, technology, [carrier]
        arr.index = self.__idx__(arr.index)
        arr.name = metric
        return arr

    def __idx__(self, _idx: pd.Index) -> Union[pd.Index, pd.MultiIndex]:
        if isinstance(_idx, pd.MultiIndex):  # carrier, technology
            if _idx.names.index("carriers") < _idx.names.index("techs"):
                _idx = _idx.swaplevel()
            _idx.names = ["technology", "carrier"]
            return _idx
        elif _idx.name == "carriers":  # carrier
            _idx.name = "carrier"
            return _idx

        # parse concatenated location, technology, [carrier] values
        msg = f"{_idx.name}: unsupported index"
        # index name: loc_techs_*, loc_tech_carriers_*
        if "loc_tech" not in _idx.name:
            raise ValueError(msg)
        idx = _idx.str.split("::", expand=True)
        if "carrier" in _idx.name:
            if idx.nlevels == 3:
                idx.names = ["location", "technology", "carrier"]
            else:
                raise ValueError(msg)
        else:
            if idx.nlevels == 2:
                idx.names = ["location", "technology"]
            else:
                raise ValueError(msg)
        return idx
