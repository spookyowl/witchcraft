#!/usr/bin/env python
from setuptools import setup

setup(
    name="witchcraft",
    version="0.2",
    description='',
    author='Peter Facka',
    author_email='pfacka@trackingwire.com',
    packages=[
        'witchcraft',
    ],
    zip_safe=False,	
    install_requires=[
	'psycopg2>=2.6.1',
	'SQLAlchemy>=1.0.6',
        'hy>=0.11.1',
    ],
    package_data={'witchcraft': ['*.txt']},
    provides=['witchcraft (0.2)'],
    include_package_data=True,
)
