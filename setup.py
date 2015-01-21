#!/usr/bin/env python

from setuptools import setup, find_packages

install_requires = []
with open("requirements.txt") as fp:
    for s in fp:
        install_requires.append(s.strip())

setup(
    name="luigi-td",
    version='0.0.0',
    description="Luigi integration for Treasure Data",
    author="Treasure Data, Inc.",
    author_email="support@treasure-data.com",
    url="http://treasuredata.com/",
    install_requires=install_requires,
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
