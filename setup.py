"""Package setup for pipewatch."""

from setuptools import setup, find_packages

with open("README.md", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="pipewatch",
    version="0.1.0",
    description="Lightweight CLI for monitoring and alerting on ETL pipeline health in real time.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="pipewatch contributors",
    python_requires=">=3.9",
    packages=find_packages(exclude=["tests*"]),
    install_requires=[
        "PyYAML>=6.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0",
            "pytest-cov>=4.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "pipewatch=pipewatch.cli:main",
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: System :: Monitoring",
        "Environment :: Console",
    ],
    include_package_data=True,
)
