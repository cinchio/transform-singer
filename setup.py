#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name="transform-singer",
    version="0.1.26",
    description="Singer.io transform for converting schemas and records between a tap and a target",
    author="Cinch",
    url="https://github.com/cinchio/transform-singer",
    python_requires='>=3.6.0',
    py_modules=["transform_singer"],
    install_requires=[
        "singer-python==5.12.1",
        "requests==2.26.0",
    ],
    entry_points="""
    [console_scripts]
    transform-singer=transform_singer:main
    """,
    packages=find_packages(include=['transform_singer', 'transform_singer.*']),
)
