"""Tests for auth."""

import sys
import unittest
from unittest import mock

from absl import flags
from absl.testing import parameterized
from googleapiclient import discovery
from xmanager.cloud import auth

_TEST_SERVICE_ACCOUNT_NAME = 'test-service-account'
_DEFAULT_SERVICE_ACCOUNT_NAME = 'xmanager'

_SERVICE_ACCOUNT_FLAG_TEST_PARAMETERS = [{
    'sys_argv': sys.argv,
    'expected_account_name': _DEFAULT_SERVICE_ACCOUNT_NAME
}, {
    'sys_argv': [
        *sys.argv, f'--xm_gcp_service_account_name={_TEST_SERVICE_ACCOUNT_NAME}'
    ],
    'expected_account_name': _TEST_SERVICE_ACCOUNT_NAME
}]


class GetServiceAccountTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    # Resets flags between runs
    flags.FLAGS.unparse_flags()

  @parameterized.parameters(_SERVICE_ACCOUNT_FLAG_TEST_PARAMETERS)
  def test_get_service_account_existing_account(self, sys_argv,
                                                expected_account_name):
    """Tests that `get_service_account` does nothing on a properly configured account."""

    flags.FLAGS(sys_argv)

    mock_service_accounts = mock.Mock()
    mock_service_accounts.list.return_value.execute.return_value = {
        'accounts': [{
            'email':
                f'{expected_account_name}@test-project.iam.gserviceaccount.com'
        }]
    }
    mock_service_accounts.create.return_value.execute.return_value = None

    mock_projects = mock.Mock()
    mock_projects.serviceAccounts.return_value = mock_service_accounts
    mock_projects.getIamPolicy.return_value.execute.return_value = {
        'bindings': [{
            'role':
                'roles/aiplatform.user',
            'members': [(f'serviceAccount:{expected_account_name}'
                         '@test-project.iam.gserviceaccount.com')]
        }, {
            'role':
                'roles/storage.admin',
            'members': [(f'serviceAccount:{expected_account_name}'
                         '@test-project.iam.gserviceaccount.com')]
        }]
    }

    mock_discovery_build = mock.Mock()
    mock_discovery_build.projects.return_value = mock_projects

    with mock.patch.object(auth, 'get_project_name',
                           return_value='test-project'), \
         mock.patch.object(discovery, 'build',
                           return_value=mock_discovery_build):
      auth.get_service_account()

    mock_service_accounts.create.assert_not_called()
    mock_projects.setIamPolicy.assert_not_called()

  @parameterized.parameters(_SERVICE_ACCOUNT_FLAG_TEST_PARAMETERS)
  def test_get_service_account_new_account(self, sys_argv,
                                           expected_account_name):
    """Tests if `get_service_account` creates a new account and permissions properly."""

    flags.FLAGS(sys_argv)

    mock_service_accounts = mock.Mock()
    mock_service_accounts.list.return_value.execute.return_value = {}
    mock_service_accounts.create.return_value.execute.return_value = None

    mock_projects = mock.Mock()
    mock_projects.serviceAccounts.return_value = mock_service_accounts
    mock_projects.getIamPolicy.return_value.execute.return_value = {
        'bindings': []
    }

    mock_discovery_build = mock.Mock()
    mock_discovery_build.projects.return_value = mock_projects

    with mock.patch.object(auth, 'get_project_name',
                           return_value='test-project'), \
         mock.patch.object(discovery, 'build',
                           return_value=mock_discovery_build):
      auth.get_service_account()

    mock_service_accounts.create.assert_called_once_with(
        name='projects/test-project',
        body={
            'accountId': expected_account_name,
            'serviceAccount': {
                'displayName': expected_account_name,
                'description': 'XManager service account'
            }
        })

    mock_projects.setIamPolicy.assert_called_once_with(
        resource='test-project',
        body={
            'policy': {
                'bindings': [{
                    'role':
                        'roles/aiplatform.user',
                    'members': [(f'serviceAccount:{expected_account_name}'
                                 '@test-project.iam.gserviceaccount.com')]
                }, {
                    'role':
                        'roles/storage.admin',
                    'members': [(f'serviceAccount:{expected_account_name}'
                                 '@test-project.iam.gserviceaccount.com')]
                }]
            }
        })

  @parameterized.parameters(_SERVICE_ACCOUNT_FLAG_TEST_PARAMETERS)
  def test_get_service_account_no_permissions(self, sys_argv,
                                              expected_account_name):
    """Tests if `get_service_account` creates permissions properly for an existing account with no permissions."""

    flags.FLAGS(sys_argv)

    mock_service_accounts = mock.Mock()
    mock_service_accounts.list.return_value.execute.return_value = {
        'accounts': [{
            'email':
                f'{expected_account_name}@test-project.iam.gserviceaccount.com'
        }]
    }
    mock_service_accounts.create.return_value.execute.return_value = None

    mock_projects = mock.Mock()
    mock_projects.serviceAccounts.return_value = mock_service_accounts
    mock_projects.getIamPolicy.return_value.execute.return_value = {
        'bindings': []
    }

    mock_discovery_build = mock.Mock()
    mock_discovery_build.projects.return_value = mock_projects

    with mock.patch.object(auth, 'get_project_name',
                           return_value='test-project'), \
         mock.patch.object(discovery, 'build',
                           return_value=mock_discovery_build):
      auth.get_service_account()

    mock_service_accounts.create.assert_not_called()

    mock_projects.setIamPolicy.assert_called_once_with(
        resource='test-project',
        body={
            'policy': {
                'bindings': [{
                    'role':
                        'roles/aiplatform.user',
                    'members': [(f'serviceAccount:{expected_account_name}'
                                 '@test-project.iam.gserviceaccount.com')]
                }, {
                    'role':
                        'roles/storage.admin',
                    'members': [(f'serviceAccount:{expected_account_name}'
                                 '@test-project.iam.gserviceaccount.com')]
                }]
            }
        })

  @parameterized.parameters(_SERVICE_ACCOUNT_FLAG_TEST_PARAMETERS)
  def test_get_service_account_some_permissions(self, sys_argv,
                                                expected_account_name):
    """Tests if `get_service_account` creates permissions properly for an existing account with some permissions."""

    flags.FLAGS(sys_argv)

    mock_service_accounts = mock.Mock()
    mock_service_accounts.list.return_value.execute.return_value = {
        'accounts': [
            {
                'email': (f'{expected_account_name}'
                          '@test-project.iam.gserviceaccount.com')
            },
            {
                # Used to test that permissions of other users are not affected
                'email': 'someone_else@test-project.iam.gserviceaccount.com'
            }
        ]
    }
    mock_service_accounts.create.return_value.execute.return_value = None

    mock_projects = mock.Mock()
    mock_projects.serviceAccounts.return_value = mock_service_accounts
    mock_projects.getIamPolicy.return_value.execute.return_value = {
        'bindings': [{
            'role':
                'roles/aiplatform.user',
            'members': [(f'serviceAccount:{expected_account_name}'
                         '@test-project.iam.gserviceaccount.com')]
        }, {
            'role': 'roles/storage.admin',
            'members': ['someone-else@test-project.iam.gserviceaccount.com']
        }]
    }

    mock_discovery_build = mock.Mock()
    mock_discovery_build.projects.return_value = mock_projects

    with mock.patch.object(auth, 'get_project_name',
                           return_value='test-project'), \
         mock.patch.object(discovery, 'build',
                           return_value=mock_discovery_build):
      auth.get_service_account()

    mock_service_accounts.create.assert_not_called()

    mock_projects.setIamPolicy.assert_called_once_with(
        resource='test-project',
        body={
            'policy': {
                'bindings': [{
                    'role':
                        'roles/aiplatform.user',
                    'members': [(f'serviceAccount:{expected_account_name}'
                                 '@test-project.iam.gserviceaccount.com')]
                }, {
                    'role':
                        'roles/storage.admin',
                    'members': [('someone-else'
                                 '@test-project.iam.gserviceaccount.com'),
                                (f'serviceAccount:{expected_account_name}'
                                 '@test-project.iam.gserviceaccount.com')]
                }]
            }
        })


if __name__ == '__main__':
  unittest.main()
