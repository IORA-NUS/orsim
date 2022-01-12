import pathlib
from setuptools import find_packages, setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()


setup(
    name='orsim',
    # packages=find_packages(include=['orsim'], ),
    packages=find_packages(),
    version='0.1.1',
    description='Distributed Agent based Simulation Library',
    url='https://pypi.org/project/orsim',
    long_description=README,
    long_description_content_type="text/markdown",
    author='iora_dev_team',
    license='MIT',
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    install_requires=[],
    setup_requires=['pytest-runner'],
    tests_require=['pytest>=4.4.1'],
    test_suite='tests',
)
