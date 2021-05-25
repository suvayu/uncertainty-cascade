"""Overrides and scenarios"""

from itertools import product
from pathlib import Path
from typing import Dict

from glom import glom, Assign


def get_demand_profiles(config: Dict, demand_profile_dir: str, output: str) -> Dict:
    _output = Path(output)
    return glom(
        {},
        tuple(
            Assign(
                f"{path.stem}.locations.{country}",
                {"exists": False}
                # NOTE: not in demand profiles from DESTINEE, but present in
                # euro-calliope
                if country in ["CYP", "MNE"]
                else glom(
                    {},
                    Assign(
                        "techs.demand_elec.constraints.resource",
                        f"file={path.relative_to(_output.parent)}:{country}",
                        missing=dict,
                    ),
                ),
                missing=dict,
            )
            for path in Path(demand_profile_dir).glob("*.csv")
            for country in config["locations"].keys()
        ),
    )


def get_time_spans() -> Dict:
    return {
        "janweek1": {"model": {"subset_time": ["2016-01-01", "2016-01-07"]}},
        "yearmin1day": {"model": {"subset_time": ["2016-01-01", "2016-12-30"]}},
        # NOTE: align time interval with capacity factor timeseries
    }


def get_scenarios(config: Dict, demand_profile_dir: str, output: str) -> Dict:
    demand_profiles = get_demand_profiles(config, demand_profile_dir, output)
    time_spans = get_time_spans()
    scenarios = {
        "scenarios": {
            f"scenario{i+1}": list(v)
            for i, v in enumerate(product(demand_profiles, ("yearmin1day",)))
        },
        "overrides": {**demand_profiles, **time_spans},
    }
    scenarios["scenarios"].update(
        {"test": [list(demand_profiles.keys())[0], "janweek1"]}
    )
    return scenarios
