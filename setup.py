
from setuptools import setup, find_packages

setup(
    name='security_data_collector',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'qradar-api',
        'cortex-xdr-client',
        'hdfs',
    ],
    entry_points={
        'console_scripts': [
            'sdc = sdc_tool.main:main',
        ],
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8',
)

