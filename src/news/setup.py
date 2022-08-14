from setuptools import setup,find_packages
import os

setup(
    version=os.environ.get("PACKAGE_VERSION","0.0.0"),
    package_dir={"":"src"},
    packages=find_packages(where="./src", exclude=("*.tests", "*.tests.*", "tests.*", "tests"))
    )