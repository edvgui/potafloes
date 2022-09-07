from setuptools import setup, find_packages

setup(
    name="patasync",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    version="0.0.1",
    license="MIT",
    install_requires=[],
)