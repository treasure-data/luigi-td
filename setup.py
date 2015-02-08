#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="luigi-td",
    version='0.6.0',
    description="Luigi integration for Treasure Data",
    author="Treasure Data, Inc.",
    author_email="support@treasure-data.com",
    url="http://treasuredata.com/",
    install_requires=open("requirements.txt").read().splitlines(),
    packages=find_packages(),
    license="Apache Software License",
    platforms="Posix; MacOS X; Windows",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Topic :: Internet",
    ],
)
