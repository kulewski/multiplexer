"""setup.py of the wheel `make wheel` builds: the multiplexer package with
its extension, plus lib.logging, the generated protocol buffer module the
package imports. Copied next to the package with the version filled in."""

from setuptools import find_packages, setup
from setuptools.dist import Distribution


class BinaryDistribution(Distribution):
    """Tags the wheel with the platform: it carries a compiled extension."""

    def has_ext_modules(self):
        return True


setup(
    distclass=BinaryDistribution,
    name="multiplexer",
    version="@VERSION@",
    description="Client and backend libraries for the multiplexer message broker",
    url="https://github.com/kulewski/multiplexer",
    license="MIT",
    packages=find_packages(),
    package_data={"multiplexer": ["_native.so"]},
    install_requires=["protobuf"],
    python_requires=">=3.10",
)
