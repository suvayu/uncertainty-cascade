"""Overrides and scenarios"""

from collections import Counter
from itertools import product, chain
from pathlib import Path
from typing import Dict

from glom import glom, Assign


def get_demand_profiles(config: Dict, demand_profile_dir: str, output: str) -> Dict:
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
