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
"""Helper methods to find the cluster details when running on CAIP."""

import json
import os
import re
from typing import List, Tuple
import urllib.request

from google.cloud import storage


def get_master_address_port() -> Tuple[str, str]:
  """Get the master worker from CLUSTER_SPEC.

  https://cloud.google.com/ai-platform/training/docs/distributed-training-containers#about-cluster-spec

  Returns:
    address string and port string
  """
  cluster_spec = os.environ.get('CLUSTER_SPEC', None)
  print('CLUSTER_SPEC:', cluster_spec)
  if not cluster_spec:
    return '127.0.0.1', '29500'

  cluster_spec = json.loads(cluster_spec)
  master = cluster_spec['cluster']['workerpool0'][0]
  [addr, port] = master.split(':')
  return addr, port


def get_world_size_rank() -> Tuple[int, int]:
  """Get the world size and rank of the current replica from CLUSTER_SPEC."""
  cluster_spec = os.environ.get('CLUSTER_SPEC', None)
  if not cluster_spec:
    return 1, 0

  cluster_spec = json.loads(cluster_spec)
  world_size = 0
  for pool in cluster_spec['cluster']:
    if pool == cluster_spec['task']['type']:
      rank = world_size + cluster_spec['task']['index']
    world_size += len(cluster_spec['cluster'][pool])

  print('WORLD SIZE,', world_size, '; RANK', rank)
  return world_size, rank


def create_cluster_specs(workers: List[str]) -> List[str]:
  """Takes a list of domain names and constructs a CLUSTER_SPEC for each."""
  cluster = {}
  for i, domain in enumerate(workers):
    cluster[f'workerpool{i}'] = [domain]
  specs = []
  for i in range(len(workers)):
    spec = {
        'cluster': cluster,
        'task': {
            'type': f'workerpool{i}',
            'index': i
        },
    }
    specs.append(json.dumps(spec))
  return specs


def get_workerpool_address(workerpool: str) -> str:
  """Creates a late-binding that is mapped at runtime."""
  return f'%objectname({workerpool})%'


def map_workerpool_address_args(args: List[str]) -> List[str]:
  """Maps late-binding to workerpool addresses at runtime."""
  cluster_spec = os.environ.get('CLUSTER_SPEC', None)
  if cluster_spec is None:
    return args

  # capture %objectname(<capture-group>)%
  late_bind_regex = re.compile(r'\%objectname\((.*)\)\%')
  cluster_spec = json.loads(cluster_spec)['cluster']
  result = []
  for arg in args:
    match = late_bind_regex.search(arg)
    if match is None:
      result.append(arg)
    else:
      worker_type = match.group(1)
      result.append(
          arg.replace(f'%objectname({worker_type})%',
                      cluster_spec[worker_type][0]))
  return result


def print_workerpool_address_args(argv: List[str]) -> None:
  for arg in map_workerpool_address_args(argv[1:]):
    print(arg,)


def create_workerpool_address_env_vars_script(path: str) -> None:
  """Create a script to map late-binding env vars to their value at runtime."""
  with open(path, 'w') as f:
    f.write('#!/bin/bash\n\n')

  cluster_spec = os.environ.get('CLUSTER_SPEC', None)
  if cluster_spec is None:
    return

  content = []
  # capture %objectname(<capture-group>)%
  late_bind_regex = re.compile(r'\%objectname\((.*)\)\%')
  cluster_spec = json.loads(cluster_spec)['cluster']
  for key, value in os.environ.items():
    match = late_bind_regex.match(value)
    if match:
      worker_type = match.group(1)
      content.append(f'export {key}={cluster_spec[worker_type][0]}')

  if content:
    with open(path, 'a') as f:
      f.write('\n'.join(content))


def get_region() -> str:
  """Get the region of the current GCE VM from metadata, e.g. us-central1."""
  # Default VM instance metadata
  # https://cloud.google.com/compute/docs/metadata/default-metadata-values#vm_instance_metadata
  request = urllib.request.Request(
      'http://metadata.google.internal/computeMetadata/v1/instance/zone')
  request.add_header('Metadata-Flavor', 'Google')
  response = urllib.request.urlopen(request)
  content = str(response.read())
  zone = content.strip('\'').split('/')[-1]
  region = zone[:-2]
  return region


def get_closest_bucket(bucket_names: List[str]) -> str:
  """Get the closest bucket from a list of buckets."""
  region = get_region()
  for name in bucket_names:
    b = storage.Bucket(client=storage.Client(), name=name)
    b.reload()
    # Only works for REGIONAL and MULT_REGIONAL.
    if region.startswith(b.location.lower()):
      return name
  return bucket_names[0]  # None of the buckets are close. Pick a random one.
