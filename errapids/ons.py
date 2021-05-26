"""Overrides and scenarios"""

from collections import Counter
from itertools import product, chain
from pathlib import Path
from typing import Dict, Iterable, List

from glom import glom, Assign, Match


def merge_dicts(confs: List[Dict]) -> Dict:
    """Merge a sequence of dictionaries

    Common keys at the same depth are recursively reconciled.  The newer value
    overwrites earlier values.  The order of the keys are preserved.  When
    merging repeated keys, the position of the first occurence is considered as
    correct.

    Parameters
    ----------
    confs: Sequence[Dict]
        A list of dictionaries

    Returns
    -------
    Dict
        Merged dictionary

    Examples
    --------

    - e & b.d tests overwriting values
    - b, e & b.d tests key ordering
    - e & e.* tests adding new sub-keys

    >>> d1 = {"a": 1, "b": {"c": 3, "d": 4}, "e": True}
    >>> d2 = {"c": 3, "b": {"e": 5, "d": 40}, "e": {"g": True, "h": "foo"}}
    >>> expected = {
    ...     "a": 1,
    ...     "b": {"c": 3, "d": 40, "e": 5},
    ...     "e": {"g": True, "h": "foo"},
    ...     "c": 3,
    ... }
    >>> result = merge_dicts([d1, d2])
    >>> result == expected
    True
    >>> list(result) == list(expected)  # key ordering preserved
    True
    >>> list(result["b"]) == list(expected["b"])  # key ordering preserved
    True

    """
    if not all(map(lambda obj: isinstance(obj, dict), confs)):
        return confs[-1]

    res = {}
    for key, count in Counter(chain.from_iterable(confs)).items():
        matches = [conf[key] for conf in confs if key in conf]
        if count > 1:
            res[key] = merge_dicts(matches)  # duplicate keys, recurse
        else:
            res[key] = matches[0]  # only one element
    return res


def get_demand_profiles(
    locations: Iterable[str], demand_profile_dir: str, output: str
) -> Dict:
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
            for country in locations
        ),
    )


def vary_costs(
    basename: str,
    ref: Dict,
    techs: List[str],
    techgrps: List[str],
    costs: List[str],
    factors: List[float],
    cost_t: str = "monetary",
) -> Dict:
    """Vary `costs` by `factors` and generate an override for every variation

    Parameters
    ----------
    basename : str
        basename for override

    ref : Dict
        Reference dictionary

    techs : List[str]
        Technology name

    techgrps : List[str]
        Technology group name

    costs : List[str]
        List of costs to vary

    factors : List[float]
        Multiplicative factors to vary the costs

    cost_t : str (default: monetary)
        Cost type

    Returns
    -------
    Dict
        A dictionary populated with all the overrides

    """
    _match = Match(
        {
            **{
                f"{tech}": {
                    "costs": {f"{cost_t}": {c: object for c in costs + [str]}},
                    str: object,
                }
                for tech in chain(techs, techgrps)
            },
            str: dict,
        }
    )
    glom(ref, _match)  # validate
    eof_techs = len(techs)
    return glom(
        {},
        tuple(
            Assign(
                f"{basename}{i}.techs.{tech}.costs.{cost_t}.{c}"
                if j < eof_techs
                else f"{basename}{i}.tech_grps.{tech}.costs.{cost_t}.{c}",
                glom(ref, f"{tech}.costs.{cost_t}.{c}") * f,
                missing=dict,
            )
            for i, f in enumerate(factors, start=1)
            for j, tech in enumerate(chain(techs, techgrps))
            for c in costs
        ),
    )


def get_time_spans() -> Dict:
    return {
        "janweek1": {"model": {"subset_time": ["2016-01-01", "2016-01-07"]}},
        "yearmin1day": {"model": {"subset_time": ["2016-01-01", "2016-12-30"]}},
        # NOTE: align time interval with capacity factor timeseries
    }


def pick_one(overrides):
    return list(overrides.keys())[0]


def get_scenarios(config: Dict, demand_profile_dir: str, output: str) -> Dict:
    demand_profiles = get_demand_profiles(
        config["locations"].keys(), demand_profile_dir, output
    )
    config = merge_dicts([config["techs"], config["tech_groups"]])
    pv_scenarios = vary_costs(
        "pv", config, ["open_field_pv"], ["pv_on_roof"], ["energy_cap"], [1, 0.7, 0.5]
    )
    batt_scenarios = vary_costs(
        "battery", config, ["battery"], [], ["energy_cap", "storage_cap"], [1, 0.7, 0.5]
    )
    time_spans = get_time_spans()
    scenarios = {
        "scenarios": {
            f"scenario{i+1:02d}": list(v)
            for i, v in enumerate(
                product(demand_profiles, pv_scenarios, batt_scenarios, ("yearmin1day",))
            )
        },
        "overrides": {
            **demand_profiles,
            **pv_scenarios,
            **batt_scenarios,
            **time_spans,
        },
    }
    scenarios["scenarios"].update(
        {
            "test": [
                pick_one(demand_profiles),
                pick_one(pv_scenarios),
                pick_one(batt_scenarios),
                "janweek1",
            ]
        }
    )
    return scenarios
