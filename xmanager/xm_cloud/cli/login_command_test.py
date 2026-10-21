# Copyright 2026 Google LLC
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
"""Unit tests for `xmanager.xm_cloud.cli.login_command`."""

from __future__ import annotations

import email.message
import io
import json
import os
import pathlib
import stat
import threading
import time
from unittest import mock
import urllib.error
import urllib.parse
import urllib.request

from absl.testing import absltest
from xmanager.xm_cloud.cli import login_command

_SERVER = 'https://xmc.example.com'


class LoginCommandTest(absltest.TestCase):
  """Unit tests for `xmanager login` and `xmanager logout`."""

  def setUp(self) -> None:
    super().setUp()
    self._xdg = self.create_tempdir().full_path

  def _env(self, **extra: str) -> dict[str, str]:
    env = {'XDG_CONFIG_HOME': self._xdg, 'XMANAGER_UI_URL': _SERVER}
    env['DISPLAY'] = ':0'
    env.update(extra)
    return env

  def test_resolve_server_precedence_and_validation(self) -> None:
    cases = [
        (
            {
                'XMC_API_URL': 'https://a.io/v1',
                'XMANAGER_UI_URL': 'https://b.io',
            },
            'https://a.io',
        ),
        ({'XMANAGER_UI_URL': 'https://x.dev/ui'}, 'https://x.dev'),
        ({'XMC_DOMAIN': 'xmc.example.com'}, 'https://xmc.example.com'),
        (
            {
                'XMANAGER_ENDPOINT': 'dns:///grpc.a.dev:443',
                'XMC_API_URL': 'https://b.dev',
            },
            'https://a.dev',
        ),
        (
            {
                'XMANAGER_ENDPOINT': 'localhost:50051',
                'XMC_API_URL': 'https://b.dev/v1',
            },
            'https://b.dev',
        ),
        ({'XMC_API_URL': 'http://127.0.0.1:8080'}, 'http://127.0.0.1:8080'),
    ]
    for env, expected in cases:
      self.assertEqual(login_command.resolve_server(env), expected)
    with self.assertRaises(login_command.AuthError) as ctx:
      login_command.resolve_server({})
    self.assertEqual(str(ctx.exception), login_command.NO_SERVER_MESSAGE)
    for bad_env in (
        {'XMC_API_URL': 'http://xmc.example.com'},
        {'XMC_API_URL': 'https://xmc.example.com?q=1'},
        {'XMANAGER_ENDPOINT': 'api.example.com:443', 'XMC_API_URL': _SERVER},
        {'XMANAGER_ENDPOINT': 'dns:///grpc.'},
    ):
      with self.assertRaises(login_command.AuthError):
        login_command.resolve_server(bad_env)

  def test_credentials_roundtrip_permissions_and_relative_xdg(self) -> None:
    self.assertEqual(
        login_command.credentials_path({'XDG_CONFIG_HOME': 'relative'}),
        pathlib.Path.home() / '.config' / 'xmc' / 'credentials.json',
    )
    self.assertEqual(
        login_command.credentials_path({'XMC_CREDENTIALS_FILE': 'relative'}),
        pathlib.Path.home() / '.config' / 'xmc' / 'credentials.json',
    )
    custom_file = os.path.join(self._xdg, 'custom_creds.json')
    self.assertEqual(
        login_command.credentials_path({'XMC_CREDENTIALS_FILE': custom_file}),
        pathlib.Path(custom_file),
    )
    path = login_command.credentials_path({'XDG_CONFIG_HOME': self._xdg})
    self.assertEqual(path, pathlib.Path(self._xdg) / 'xmc' / 'credentials.json')
    self.assertEqual(login_command.load_credentials(path), {})
    path.parent.mkdir(parents=True, exist_ok=True)
    if os.name == 'posix':
      os.chmod(path.parent, 0o755)
    entry = {'access_token': 't', 'email': 'u@example.com', 'expires_at': 999}
    login_command.save_credentials(
        path, {'https://z.example.com': entry, _SERVER: entry}
    )
    raw = path.read_text(encoding='utf-8')
    self.assertLess(raw.index(_SERVER), raw.index('https://z.example.com'))
    self.assertEqual(
        login_command.load_credentials(path),
        {'https://z.example.com': entry, _SERVER: entry},
    )
    if os.name == 'posix':
      self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
      self.assertEqual(stat.S_IMODE(path.parent.stat().st_mode), 0o700)
      os.chmod(path, 0o644)
      with self.assertRaisesRegex(
          login_command.AuthError, 'xmanager logout --all'
      ):
        login_command.load_credentials(path)

  def test_pkce_and_browser_availability(self) -> None:
    verifier, challenge = login_command.pkce_pair()
    self.assertGreaterEqual(len(verifier), 43)
    self.assertNotIn('=', challenge)
    self.assertTrue(login_command.browser_available({'DISPLAY': ':0'}))
    self.assertFalse(
        login_command.browser_available(
            {'DISPLAY': ':0', 'SSH_CONNECTION': '1.2.3.4 55 1.2.3.5 22'}
        )
    )

  def test_await_loopback_code_handles_forged_and_genuine_callbacks(
      self,
  ) -> None:
    opener = urllib.request.build_opener(login_command._NoRedirect)

    def open_url(login_url: str) -> bool:
      qs = urllib.parse.parse_qs(urllib.parse.urlsplit(login_url).query)
      port, state = int(qs['port'][0]), qs['state'][0]

      def send_callbacks() -> None:
        with self.assertRaises(urllib.error.HTTPError) as bad:
          opener.open(f'http://127.0.0.1:{port}/callback?state=wrong&code=bad')
        self.assertEqual(bad.exception.code, 400)
        with self.assertRaises(urllib.error.HTTPError) as empty:
          opener.open(f'http://127.0.0.1:{port}/callback?state={state}')
        self.assertEqual(empty.exception.code, 400)
        with self.assertRaises(urllib.error.HTTPError) as redirect:
          opener.open(
              f'http://127.0.0.1:{port}/callback?state={state}&code=good-code'
          )
        self.assertEqual(redirect.exception.code, 302)

      threading.Thread(target=send_callbacks, daemon=True).start()
      return True

    code = login_command.await_loopback_code(
        _SERVER, 'challenge-xyz', open_url, io.StringIO(), timeout=5.0
    )
    self.assertEqual(code, 'good-code')

    def open_url_error(login_url: str) -> bool:
      qs = urllib.parse.parse_qs(urllib.parse.urlsplit(login_url).query)
      port, state = int(qs['port'][0]), qs['state'][0]

      def send_err() -> None:
        with self.assertRaises(urllib.error.HTTPError) as redirect:
          opener.open(
              f'http://127.0.0.1:{port}/callback?state={state}&error=denied'
          )
        self.assertEqual(redirect.exception.code, 302)

      threading.Thread(target=send_err, daemon=True).start()
      return True

    with self.assertRaisesRegex(login_command.AuthError, 'denied'):
      login_command.await_loopback_code(
          _SERVER, 'challenge-xyz', open_url_error, io.StringIO(), timeout=5.0
      )

  def test_exchange_code_success_and_http_error(self) -> None:
    body = json.dumps({
        'access_token': 'jwt-token',
        'token_type': 'Bearer',
        'expires_in': 3600,
        'email': 'researcher@example.com',
    }).encode('utf-8')
    fake_resp = io.BytesIO(body)
    fake_resp.__enter__ = lambda s: s  # type: ignore[attr-defined]
    fake_resp.__exit__ = lambda *args: None  # type: ignore[attr-defined]
    with mock.patch.object(urllib.request, 'urlopen', return_value=fake_resp):
      grant = login_command.exchange_code(_SERVER, 'code-1', 'verifier-1')
    self.assertEqual(grant['access_token'], 'jwt-token')
    http_err = urllib.error.HTTPError(
        f'{_SERVER}/api/cli/token',
        400,
        'Bad Request',
        email.message.Message(),
        io.BytesIO(b'{}'),
    )
    with mock.patch.object(urllib.request, 'urlopen', side_effect=http_err):
      with self.assertRaises(login_command.AuthError):
        login_command.exchange_code(_SERVER, 'code-1', 'verifier-1')

  def test_probe_cli_login_supports_new_and_rejects_old_xmc(self) -> None:
    def _http_err(code: int) -> urllib.error.HTTPError:
      return urllib.error.HTTPError(
          f'{_SERVER}/api/cli/login',
          code,
          'err',
          email.message.Message(),
          io.BytesIO(b''),
      )

    with mock.patch.object(
        urllib.request.OpenerDirector, 'open', side_effect=_http_err(400)
    ):
      login_command._probe_cli_login(_SERVER)
    for old_code in (401, 404):
      with mock.patch.object(
          urllib.request.OpenerDirector, 'open', side_effect=_http_err(old_code)
      ):
        with self.assertRaisesRegex(
            login_command.AuthError, 'requires XMC >= 0.16.0'
        ):
          login_command._probe_cli_login(_SERVER)
    with mock.patch.object(
        urllib.request.OpenerDirector, 'open', side_effect=_http_err(302)
    ):
      with self.assertRaisesRegex(login_command.AuthError, 'HTTP 302'):
        login_command._probe_cli_login(_SERVER)
    with mock.patch.object(
        urllib.request.OpenerDirector, 'open', side_effect=OSError('down')
    ):
      with self.assertRaisesRegex(login_command.AuthError, 'Cannot reach'):
        login_command._probe_cli_login(_SERVER)

  def test_run_login_manual_status_print_token_and_logout(self) -> None:
    out, err, env = io.StringIO(), io.StringIO(), self._env()
    grant = {'access_token': 'jwt', 'email': 'u@example.com', 'expires_in': 900}
    no_open = mock.Mock(side_effect=AssertionError('unexpected open_url'))
    with (
        mock.patch.object(login_command, '_probe_cli_login') as mock_probe,
        mock.patch.object(login_command, 'exchange_code', return_value=grant),
    ):
      self.assertEqual(
          login_command.run_login(
              ['--no-browser'],
              env=env,
              out=out,
              err=err,
              open_url=no_open,
              prompt=lambda _: 'c',
          ),
          0,
      )
      self.assertNotIn('No local browser detected', out.getvalue())
      ssh_out = io.StringIO()
      self.assertEqual(
          login_command.run_login(
              [],
              env=self._env(SSH_CONNECTION='1.2.3.4 55 1.2.3.5 22'),
              out=ssh_out,
              err=err,
              open_url=no_open,
              prompt=lambda _: 'c',
          ),
          0,
      )
      self.assertIn('No local browser detected', ssh_out.getvalue())
      rc = login_command.run_login(
          [],
          env=env,
          out=out,
          err=err,
          open_url=lambda _: False,
          prompt=lambda _: 'c',
      )
    self.assertEqual(mock_probe.call_count, 3)
    self.assertEqual(rc, 0)
    self.assertIn('falling back to manual login', out.getvalue())
    status_out, token_out = io.StringIO(), io.StringIO()
    self.assertEqual(
        login_command.run_login(['--status'], env=env, out=status_out, err=err),
        0,
    )
    self.assertIn('u@example.com', status_out.getvalue())
    self.assertNotIn('jwt', status_out.getvalue())
    self.assertEqual(
        login_command.run_login(
            ['--print-token'], env=env, out=token_out, err=err
        ),
        0,
    )
    self.assertEqual(token_out.getvalue(), 'jwt\n')
    self.assertEqual(login_command.run_logout([], env=env, out=out, err=err), 0)
    self.assertEqual(
        login_command.run_login(['--status'], env=env, out=out, err=err), 1
    )

  def test_expired_or_malformed_token_and_logout_all(self) -> None:
    env = self._env()
    path = login_command.credentials_path(env)
    for bad_entry in (
        {
            'access_token': 'old',
            'email': 'u@x.io',
            'expires_at': int(time.time()) - 10,
        },
        {'access_token': 'old', 'email': 'u@x.io', 'expires_at': 'not-an-int'},
        {'access_token': 'old', 'expires_at': int(time.time()) + 900},
        'not-a-dict',
    ):
      login_command.save_credentials(path, {_SERVER: bad_entry})  # type: ignore[dict-item]
      out, err = io.StringIO(), io.StringIO()
      self.assertEqual(
          login_command.run_login(['--print-token'], env=env, out=out, err=err),
          1,
      )
      self.assertEqual(
          login_command.run_login(['--status'], env=env, out=out, err=err), 1
      )
    self.assertEqual(
        login_command.run_logout(
            ['--all'], env=env, out=io.StringIO(), err=io.StringIO()
        ),
        0,
    )
    self.assertEqual(login_command.load_credentials(path), {})


if __name__ == '__main__':
  absltest.main()
