import os
from setuptools import setup, find_packages

with open(os.path.join('bilbo', 'version.py'), 'rt') as f:
    version = f.read().strip()
    version = version.split('=')[1].strip("'")

setup(
    name='bilbo',
    version=version,
    author='haje01',
    python_requires='>=3.5',
    entry_points={
        'console_scripts': [
            'bilbo = bilbo.cli:main'
        ]
    },
    install_requires=[
        'click',
        'paramiko',
        'jsonschema==3.2.0',
        'boto3'
    ]
)
