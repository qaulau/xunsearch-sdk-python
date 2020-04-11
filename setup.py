# -*- encoding: utf-8 -*-
# 
from setuptools import setup


with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='xunsearch',
    version='0.0.2',
    description='Python version of xunsearchd client (Python API)',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='qaulau',
    author_email='qaulau@hotmail.com',
    url='https://github.com/qaulau/xunsearch-sdk-python',
    packages=['xunsearch'],
    classifiers = [
	    'Development Status :: 4 - Beta',
	    'Intended Audience :: Developers',
	    'Topic :: Software Development :: Libraries :: Python Modules',
	    'License :: OSI Approved :: MIT License',
	    'Programming Language :: Python :: 2',
	    'Programming Language :: Python :: 2.6',
	    'Programming Language :: Python :: 2.7',
	]
)