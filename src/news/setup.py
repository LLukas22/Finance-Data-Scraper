from setuptools import setup
import os

setup(version=os.environ.get("PACKAGE_VERSION","DEBUG"))