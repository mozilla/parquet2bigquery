#!/usr/bin/env python

from setuptools import setup


setup(
    name='parquet2bigquery',
    use_scm_version=True,
    description='A tool to load parquet data into BigQuery',
    author='Jason Thomas',
    author_email='jthomas@mozilla.com',
    scripts=['bulkload'],
    packages=['parquet2bigquery'],
    url='https://github.com/mozilla/parquet2bigquery',
    classifiers=[
        # complete classifier list: http://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)',
        'Operating System :: Unix',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Utilities',
    ],
    install_requires=[
        'beautifulsoup4==4.6.3',
        'cachetools==2.1.0',
        'certifi==2018.10.15',
        'chardet==3.0.4',
        'google==2.0.1',
        'google-api-core==1.8.0',
        'google-auth==1.5.1',
        'google-cloud==0.34.0',
        'google-cloud-bigquery==1.10.0',
        'google-cloud-core==0.29.1',
        'google-cloud-storage==1.14.0',
        'google-resumable-media==0.3.1',
        'googleapis-common-protos==1.5.3',
        'idna==2.7',
        'numpy==1.15.2',
        'protobuf==3.6.1',
        'pyasn1==0.4.4',
        'pyasn1-modules==0.2.2',
        'python-dateutil==2.7.3',
        'pytz==2018.5',
        'requests==2.21.0',
        'rsa==4.0',
        'six==1.11.0',
        'thrift==0.11.0',
        'urllib3==1.24.2',
    ]
)
