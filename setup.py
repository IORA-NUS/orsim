import pathlib
from setuptools import find_packages, setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.rst").read_text()


setup(
    name='orsim',
    # packages=find_packages(include=['orsim'], ),
    packages=find_packages(),
    version='0.2.1',
    description='Distributed Agent based Simulation Library',
    url='https://github.com/IORA-NUS/orsim',
    long_description=README,
    long_description_content_type="text/markdown",
    author='iora_dev_team',
    license='MIT',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Science/Research',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
    ],
    install_requires=[],
    setup_requires=['pytest-runner'],
    tests_require=['pytest>=4.4.1'],
    test_suite='tests',
)
