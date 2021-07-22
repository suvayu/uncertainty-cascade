"""Uncertainty cascade

"""

from pathlib import Path

from setuptools import setup, find_packages

requirements = list(
    filter(
        lambda i: "git://" not in i,
        Path("requirements.txt").read_text().strip().split("\n"),
    )
)

setup(
    name="errapids",
    version="0.3",
    description="Propagate uncertainty across models",
    packages=find_packages(exclude=["doc", "testing", "tests", "data", "expt", "tmp"]),
    install_requires=requirements,
)
