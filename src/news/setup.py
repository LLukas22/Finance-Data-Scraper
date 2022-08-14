from setuptools import setup,find_packages
import os

setup(
    version=os.environ.get("PACKAGE_VERSION","DEBUG"),
    packages=find_packages(exclude=("tests",)),
    include_package_data=True, 
    exclude_package_data={'': ['tests']}
    )