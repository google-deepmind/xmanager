import asyncio
import threading
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
import docker.errors
import requests.exceptions
from xmanager.docker import docker_adapter
from xmanager.xm_local import handles


class ContainerHandleTest(parameterized.TestCase):
  def test_client_container_is_retained_until_waited(self) -> None:
    client = mock.Mock()
    model = client.containers.run.return_value
    model.wait.return_value = {'StatusCode': 0}
    adapter = docker_adapter.DockerAdapter(client)
    container = adapter.run_container_client(
        name='test-job',
        image_id='test-image',
        args=[],
        env_vars={},
        network='xmanager',
        ports={},
        volumes={},
        gpu_count=0,
    )
    self.assertFalse(client.containers.run.call_args.kwargs['remove'])
    model.remove.assert_not_called()
    handle = handles.ContainerHandle('test-job', container, False)
    self.addCleanup(handle.futures_executor.shutdown)
    asyncio.run(handle.wait())
    self.assertEqual(model.mock_calls, [mock.call.wait(), mock.call.remove()])

  def test_nonzero_exit_fails(self) -> None:
    model = mock.Mock()
    model.wait.return_value = {'StatusCode': 7}
    handle = handles.ContainerHandle('failed', model, False)
    self.addCleanup(handle.futures_executor.shutdown)
    with self.assertRaisesRegex(RuntimeError, 'status `7`'):
      asyncio.run(handle.wait())
    with self.assertRaisesRegex(RuntimeError, 'status `7`'):
      asyncio.run(handle.wait())
    model.wait.assert_called_once()
    model.remove.assert_called_once()

  def test_success_requires_observed_zero_exit_status(self) -> None:
    model = mock.Mock()
    model.wait.return_value = {'StatusCode': 0}
    handle = handles.ContainerHandle('successful', model, False)
    self.addCleanup(handle.futures_executor.shutdown)
    asyncio.run(handle.wait())
    model.wait.side_effect = RuntimeError('Container disappeared')
    asyncio.run(handle.wait())
    model.wait.assert_called_once()
    model.remove.assert_called_once()

  def test_unobserved_removed_container_fails(self) -> None:
    model = mock.Mock()
    model.wait.side_effect = docker.errors.NotFound('Container disappeared')
    handle = handles.ContainerHandle('missing', model, False)
    self.addCleanup(handle.futures_executor.shutdown)
    with self.assertRaises(docker.errors.NotFound):
      asyncio.run(handle.wait())

  @parameterized.named_parameters(
      ('api_error', docker.errors.APIError),
      ('connection_error', requests.exceptions.ConnectionError),
      ('timeout', requests.exceptions.Timeout),
  )
  def test_cleanup_failure_preserves_success(self, error_type) -> None:
    model = mock.Mock()
    model.wait.return_value = {'StatusCode': 0}
    model.remove.side_effect = error_type('Cleanup failed')
    handle = handles.ContainerHandle('successful', model, False)
    self.addCleanup(handle.futures_executor.shutdown)
    with self.assertLogs(level='WARNING') as logs:
      asyncio.run(handle.wait())
      asyncio.run(handle.wait())
    self.assertLen(logs.records, 1)
    self.assertIn('successful', logs.records[0].getMessage())
    self.assertIs(logs.records[0].exc_info[1], model.remove.side_effect)
    model.wait.assert_called_once()
    model.remove.assert_called_once()

  @parameterized.named_parameters(
      ('api_error', docker.errors.APIError),
      ('connection_error', requests.exceptions.ConnectionError),
      ('timeout', requests.exceptions.Timeout),
  )
  def test_cleanup_failure_preserves_nonzero_exit(self, error_type) -> None:
    model = mock.Mock()
    model.wait.return_value = {'StatusCode': 7}
    model.remove.side_effect = error_type('Cleanup failed')
    handle = handles.ContainerHandle('failed', model, False)
    self.addCleanup(handle.futures_executor.shutdown)
    with self.assertLogs(level='WARNING') as logs:
      with self.assertRaisesRegex(RuntimeError, 'status `7`'):
        asyncio.run(handle.wait())
      with self.assertRaisesRegex(RuntimeError, 'status `7`'):
        asyncio.run(handle.wait())
    self.assertLen(logs.records, 1)
    self.assertIn('failed', logs.records[0].getMessage())
    self.assertIs(logs.records[0].exc_info[1], model.remove.side_effect)
    model.wait.assert_called_once()
    model.remove.assert_called_once()

  def test_cancelled_waiter_preserves_shared_exit_status(self) -> None:
    model = mock.Mock()
    finished = threading.Event()
    model.wait.side_effect = lambda: (
        {'StatusCode': 0} if finished.wait(timeout=5) else {'StatusCode': 7}
    )
    handle = handles.ContainerHandle('shared', model, False)
    self.addCleanup(handle.futures_executor.shutdown)

    async def wait() -> None:
      first = asyncio.create_task(handle.wait())
      second = asyncio.create_task(handle.wait())
      await asyncio.sleep(0)
      first.cancel()
      with self.assertRaises(asyncio.CancelledError):
        await first
      finished.set()
      await second

    asyncio.run(wait())
    model.wait.assert_called_once()
    model.remove.assert_called_once()

  def test_termination_removes_stopped_container(self) -> None:
    model = mock.Mock()
    handle = handles.ContainerHandle('stopped', model, False)
    self.addCleanup(handle.futures_executor.shutdown)
    handle.terminate()
    self.assertEqual(model.mock_calls, [mock.call.stop(), mock.call.remove()])

  def test_removal_race_preserves_exit_status(self) -> None:
    model = mock.Mock()
    model.wait.return_value = {'StatusCode': 7}
    model.remove.side_effect = docker.errors.NotFound('Container disappeared')
    handle = handles.ContainerHandle('failed', model, False)
    self.addCleanup(handle.futures_executor.shutdown)
    with self.assertRaisesRegex(RuntimeError, 'status `7`'):
      asyncio.run(handle.wait())

  def test_termination_tolerates_an_already_removed_container(self) -> None:
    model = mock.Mock()
    model.stop.side_effect = docker.errors.NotFound('Container disappeared')
    model.remove.side_effect = docker.errors.NotFound('Container disappeared')
    handle = handles.ContainerHandle('removed', model, False)
    self.addCleanup(handle.futures_executor.shutdown)
    handle.terminate()
    model.remove.assert_called_once()

  def test_subprocess_without_a_container_model_needs_no_wait(self) -> None:
    handle = handles.ContainerHandle('subprocess', None, False)
    self.addCleanup(handle.futures_executor.shutdown)
    asyncio.run(handle.wait())


if __name__ == '__main__':
  absltest.main()
