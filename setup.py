# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open('README.rst') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='pyred',
    version='0.0.9',
    description='Easily send data to Amazon Redshift',
    long_description=readme,
    author='Dacker',
    author_email='hello@dacker.co',
    url='https://github.com/dacker-team/pyred',
    license=license,
    keywords='send data amazon redshift easy',
    packages=find_packages(exclude=('tests', 'docs')),
    python_requires='>=3',

)

