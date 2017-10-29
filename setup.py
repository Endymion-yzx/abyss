from setuptools import find_packages, setup

PACKAGE_NAME = "abyss"
VERSION = '0.0.1'

# To install the library, run the following
#
# python setup.py install
#
# prerequisite: setuptools
# http://pypi.python.org/pypi/setuptools

setup(
    name=PACKAGE_NAME,
    version=VERSION,
    packages=find_packages(),
    description="Kubernetes TensorFlow API",
    author_email="",
    author="",
    # packages=['abyss'],
    include_package_data=True
)
