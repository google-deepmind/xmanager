# Copyright 2021 DeepMind Technologies Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Setup configuration specifying XManager dependencies."""

import os
from setuptools import find_namespace_packages
from setuptools import setup

with open('README.md', 'r', encoding='utf-8') as fh:
  long_description = fh.read()

_CONSOLE_SCRIPTS = [
    'xmanager = xmanager.cli.cli:entrypoint',
]
if os.path.isdir(os.path.join(os.path.dirname(__file__) or '.', 'xdash')):
  _CONSOLE_SCRIPTS.append('xdash = xdash.cli:main')

setup(
    name='xmanager',
    version='0.8.2',
    description='A framework for managing machine learning experiments',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='DeepMind Technologies Limited',
    packages=find_namespace_packages(exclude=['examples.*']),
    package_data={'': ['*.sh', '*.sql', '*.ini', '*.mako', 'py.typed']},
    python_requires='>=3.10',
    install_requires=[
        'absl-py',
        'alembic>=1.13,<2.0',
        'async_generator',
        'attrs',
        'cloud-sql-python-connector',
        'docker',
        'etils[epath]',
        'GitPython',
        'google-api-core',
        'google-api-python-client',
        'google-auth',
        'google-cloud-aiplatform',
        'google-cloud-storage',
        'googleapis-common-protos',
        'grpcio',
        'humanize',
        'immutabledict',
        'kubernetes',
        'protobuf<7',
        'psutil',
        'python-dotenv',
        'pyyaml',
        'requests',
        'sqlalchemy>=2.0,<3.0',
        'sqlparse',
        'tabulate',
        'termcolor',
    ],
    entry_points={
        'console_scripts': _CONSOLE_SCRIPTS,
    },
    # https://github.com/pypa/warehouse/blob/de4a2e5e2ec26d01bf7813da427ebc4725dccde9/warehouse/templates/packaging/detail.html#L20-L60
    project_urls={
        'Homepage': 'https://github.com/deepmind/xmanager',
        'Issue tracker': 'https://github.com/deepmind/xmanager/issues',
    },
)
