from setuptools import find_packages, setup

setup(
    name='orsim',
    # packages=find_packages(include=['orsim'], ),
    packages=find_packages(),
    version='0.1.0',
    description='Distributed Agent based Simulation Library',
    author='iora_dev_team',
    license='MIT',
    install_requires=[],
    setup_requires=['pytest-runner'],
    tests_require=['pytest>=4.4.1'],
    test_suite='tests',
)
