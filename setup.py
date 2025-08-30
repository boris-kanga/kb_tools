# -*- coding: utf-8 -*-
from setuptools import find_packages, setup

VERSION = "v0.0.0"

DESCRIPTION = "Useful tools that can be use in all python project"
LONG_DESCRIPTION = ""

# Setting up
setup(
    name="kb_tools",
    version=VERSION.replace("v", ""),
    author="Boris Parfait Kouakou KANGA",
    author_email="kangaborisparfait@gmail.com",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION or DESCRIPTION,
    packages=find_packages(),
    install_requires=[
        [r.strip() for r in open("requirements.txt").readlines()]
    ],
    python_requires=">=3.9",
    include_package_data=True,
)
