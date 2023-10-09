#!/usr/bin/env python
import sys

# require a supported version of Python
if sys.version_info < (3, 8):
    print("Error: dbt does not support this version of Python.")
    print("Please upgrade to Python 3.8 or higher.")
    sys.exit(1)

try:
    from setuptools import find_namespace_packages
except ImportError:
    # the user has a downlevel version of setuptools.
    print("Error: dbt requires setuptools v40.1.0 or higher.")
    print('Please upgrade setuptools with "pip install --upgrade setuptools" and try again')
    sys.exit(1)

from pathlib import Path
from setuptools import setup


# pull the long description from the README
README = Path(__file__).parent / "README.md"

# used for this adapter's version and in determining the compatible dbt-core version
VERSION = Path(__file__).parent / "dbt/adapters/bigquery/__version__.py"


def _dbt_bigquery_version() -> str:
    """
    Pull the package version from the main package version file
    """
    attributes = {}
    exec(VERSION.read_text(), attributes)
    return attributes["version"]


# require a compatible minor version (~=) and prerelease if this is a prerelease
def _dbt_core_version(plugin_version: str) -> str:
    """
    Determine the compatible version of dbt-core using this package's version
    """
    try:
        # *_ may indicate a dev release which won't affect the core version needed
        major, minor, plugin_patch, *_ = plugin_version.split(".", maxsplit=3)
    except ValueError:
        raise ValueError(f"Invalid version: {plugin_version}")

    pre_release_phase = "".join([i for i in plugin_patch if not i.isdigit()])
    if pre_release_phase:
        if pre_release_phase not in ["a", "b", "rc"]:
            raise ValueError(f"Invalid version: {plugin_version}")
        core_patch = f"0{pre_release_phase}1"
    else:
        core_patch = "0"

    return f"{major}.{minor}.{core_patch}"


package_name = "dbt-bigquery"
package_version = "1.6.7"
dbt_core_version = _dbt_core_version(_dbt_bigquery_version())
description = """The BigQuery adapter plugin for dbt"""

setup(
    name="dbt-bigquery",
    version=_dbt_bigquery_version(),
    description="The Bigquery adapter plugin for dbt",
    long_description=README.read_text(),
    long_description_content_type="text/markdown",
    author="dbt Labs",
    author_email="info@dbtlabs.com",
    url="https://github.com/dbt-labs/dbt-bigquery",
    packages=find_namespace_packages(include=["dbt", "dbt.*"]),
    include_package_data=True,
    install_requires=[
        f"dbt-core~={_dbt_core_version(_dbt_bigquery_version())}",
        "google-cloud-bigquery~=3.0",
        "google-cloud-storage~=2.4",
        "google-cloud-dataproc~=5.0",
    ],
    zip_safe=False,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
)
