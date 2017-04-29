from os import path

from setuptools import setup, find_packages

here = path.abspath(path.dirname(__file__))

with open(path.join(here, "README.rst"), encoding="UTF-8") as readme:
    long_description = readme.read()

setup(
    name="datapipelines",
    version="0.0.1.dev4",
    author="Meraki Analytics Team",
    author_email="team@merakianalytics.com",
    url="https://github.com/meraki-analytics/datapipelines",
    description="Caching abstraction layer for orchestrating multiple cache tiers",
    long_description=long_description,
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Database :: Front-Ends"
    ],
    license="MIT",
    packages=find_packages(),
    zip_safe=True,
    install_requires=[
        "merakicommons", "networkx"
    ],
    extras_require={
        "testing": ["pytest", "flake8"]
    }
)
