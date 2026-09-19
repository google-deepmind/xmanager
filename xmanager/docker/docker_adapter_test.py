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

from unittest import mock

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
from xmanager import xm_flags
from xmanager.docker import docker_adapter


class DockerAdapterTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    docker_adapter.instance.cache_clear()
    self.addCleanup(docker_adapter.instance.cache_clear)
    saved_flags = flagsaver.save_flag_values()
    self.addCleanup(flagsaver.restore_flag_values, saved_flags)
    flags.FLAGS['xm_docker_client_timeout_seconds'].parse(
        str(xm_flags.DOCKER_CLIENT_TIMEOUT_SECONDS.default)
    )

  def test_default_timeout(self):
    with mock.patch.object(docker_adapter.docker, 'from_env') as from_env:
      adapter = docker_adapter.instance()

    from_env.assert_called_once_with(timeout=600)
    self.assertIs(adapter.get_client(), from_env.return_value)

  def test_configured_timeout(self):
    with flagsaver.as_parsed(xm_docker_client_timeout_seconds='1200'):
      with mock.patch.object(docker_adapter.docker, 'from_env') as from_env:
        adapter = docker_adapter.instance()

    from_env.assert_called_once_with(timeout=1200)
    self.assertIs(adapter.get_client(), from_env.return_value)

  def test_invalid_timeout_is_rejected(self):
    for value in ('invalid', '', '1.5', '0', '-1'):
      with self.subTest(value=value):
        with self.assertRaises(flags.IllegalFlagValueError):
          with flagsaver.as_parsed(xm_docker_client_timeout_seconds=value):
            self.fail('Invalid timeout accepted')

  def test_instance_reuses_client(self):
    with mock.patch.object(docker_adapter.docker, 'from_env') as from_env:
      adapter = docker_adapter.instance()
      with flagsaver.as_parsed(xm_docker_client_timeout_seconds='1200'):
        self.assertIs(docker_adapter.instance(), adapter)

    from_env.assert_called_once_with(timeout=600)


if __name__ == '__main__':
  absltest.main()
