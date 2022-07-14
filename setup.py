#!/usr/bin/env python

import setuptools


setuptools.setup(
    name="sqlalchemy_fsm",
    packages=setuptools.find_packages(where="src"),
    package_dir={"": "src"},
    py_modules=["sqlalchemy_fsm"],
    description="Finite state machine field for sqlalchemy",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Ilja & Peter",
    author_email="ilja@wise.fish",
    license="MIT",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Topic :: Database",
    ],
    keywords="sqlalchemy finite state machine fsm",
    version="2.0.11",
    url="https://github.com/VRGhost/sqlalchemy-fsm",
    install_requires=[
        "SQLAlchemy>=1.0.0",
        "six>=1.10.0",
    ],
    python_requires=">=3.6",
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
)
