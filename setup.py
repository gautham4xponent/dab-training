from setuptools import setup, find_packages

setup(
  name="dab_training",
  version="0.1.0",
  packages=find_packages(where="src"),
  package_dir={"": "src"},
  install_requires=[],
  entry_points={
    "console_scripts": [
      "main=dab_training.transform:main"  # the wheel task entry point
    ]
  },
)