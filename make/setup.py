"""setup.py of the wheel `make wheel` builds: the multiplexer package with
its extension, plus lib.logging, the generated protocol buffer module the
package imports. Copied next to the package with the version filled in,
together with README.md, which becomes the PyPI page with its links made
absolute. The distribution is `mx-multiplexer`, because `multiplexer` on
PyPI belongs to an unrelated package; the import is `multiplexer` either
way."""

import os
import re

from setuptools import find_packages, setup
from setuptools.dist import Distribution

# The release workflow passes the tag, v2.3.0; setuptools normalises that
# to 2.3.0 for the wheel, and the links below want the same, with one v.
VERSION = "@VERSION@".lstrip("v")
REPOSITORY = "https://github.com/kulewski/multiplexer"


class BinaryDistribution(Distribution):
    """Tags the wheel with the platform: it carries a compiled extension."""

    def has_ext_modules(self):
        return True


def readme_for_pypi():
    """README.md with every relative link and image pointing at the tagged
    tree on GitHub, so that the links work on the PyPI page."""
    with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"), encoding="utf-8") as readme:
        text = readme.read()
    tree = "%s/blob/v%s/" % (REPOSITORY, VERSION)
    raw = "https://raw.githubusercontent.com/kulewski/multiplexer/v%s/" % VERSION
    text = re.sub(r'src="(?!https?://)([^"]+)"', lambda m: 'src="%s%s"' % (raw, m.group(1)), text)
    text = re.sub(
        r"!\[([^\]]*)\]\((?!https?://)([^)\s]+)\)", lambda m: "![%s](%s%s)" % (m.group(1), raw, m.group(2)), text
    )
    text = re.sub(r"\]\((?!https?://|#|mailto:)([^)\s]+)\)", lambda m: "](%s%s)" % (tree, m.group(1)), text)
    return text


setup(
    distclass=BinaryDistribution,
    name="mx-multiplexer",
    version=VERSION,
    description="Client and backend libraries for Multiplexer, a stateless TCP message broker between services",
    long_description=readme_for_pypi(),
    long_description_content_type="text/markdown",
    author="Krzysztof Kulewski",
    url=REPOSITORY,
    project_urls={
        "Documentation": "%s/blob/v%s/docs/README.md" % (REPOSITORY, VERSION),
        "Source": REPOSITORY,
        "Releases": REPOSITORY + "/releases",
        "Issues": REPOSITORY + "/issues",
    },
    license="MIT",
    keywords="message broker, request reply, publish subscribe, rpc, microservices, asio, protobuf",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: C++",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
        "Topic :: System :: Networking",
        "Topic :: Software Development :: Libraries",
        "Typing :: Typed",
    ],
    packages=find_packages(),
    package_data={
        "multiplexer": ["_native.so", "py.typed", "*.pyi"],
        "multiplexer.testing": ["*.pyi"],
        "lib.logging": ["*.pyi"],
    },
    install_requires=["protobuf"],
    python_requires=">=3.10",
)
