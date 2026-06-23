"""Utilities for pushing experiment code snapshots to git."""
import dataclasses
import logging
import os
import sys

import git
import psutil


_SHELL_NAMES = ('bash', 'fish', 'zsh')


@dataclasses.dataclass(frozen=True)
class RepoStateSnapshot:
  commit_hash: str | None = None
  commit_url: str | None = None
  repo_root: str | None = None


@dataclasses.dataclass(frozen=True)
class LaunchScriptSnapshot:
  launch_script_path: str | None = None
  launch_script_url: str | None = None
  launch_script_content: str | None = None


def _get_origin_url(repo: git.Repo) -> str | None:
  """Returns the origin URL of the repository."""
  try:
    git_url = repo.remotes.origin.url
  except AttributeError:
    # No origin remote.
    return None

  if git_url.startswith('git@'):
    git_url = 'https://' + git_url[4:].replace(':', '/', 1)

  return git_url


def _get_commit_url(repo: git.Repo, commit_hash: str) -> str | None:
  """Returns a web URL to view the commit, if one can be constructed."""
  git_url = _get_origin_url(repo)
  if not git_url:
    return None

  if git_url.startswith('https://'):
    repo_url = git_url
    if repo_url.endswith('.git'):
      repo_url = repo_url[:-4]

    # Gitlab URLs look like https://gitlab.com/user/repo/-/commit/hash, while
    # Github URLs look like https://github.com/user/repo/commit/hash.
    if 'gitlab' in repo_url:
      return f'{repo_url}/-/commit/{commit_hash}'
    return f'{repo_url}/commit/{commit_hash}'
  return None


def push_repo_state(
    experiment_id: int, repo_path: str | None = None
) -> RepoStateSnapshot:
  """Pushes a traceable commit of the repo state to git remote.

  The local repository is restored to its original state, exactly as it was
  before the call to this function, including the distinction between staged
  and unstaged changes.

  Args:
    experiment_id: The ID of the experiment.
    repo_path: The path to the repository. If not specified, the current working
      directory is used.

  Returns:
    A RepoStateSnapshot with the commit hash and URL of the pushed snapshot.
  """
  try:
    repo = git.Repo(repo_path or os.getcwd(), search_parent_directories=True)
  except (git.InvalidGitRepositoryError, git.NoSuchPathError):
    logging.warning(
        'Failed to push repo state for experiment %d: not a git repository.',
        experiment_id,
    )
    return RepoStateSnapshot()
  try:
    original_head = repo.head.commit
  except ValueError:
    logging.warning(
        'Failed to push repo state for experiment %d: repository has no'
        ' commits.',
        experiment_id,
    )
    return RepoStateSnapshot()
  was_detached = repo.head.is_detached
  original_branch = None if was_detached else repo.active_branch
  commit_hash = original_head.hexsha
  ref = f'refs/xm-snapshots/launch-{experiment_id}'
  saved_stash = False
  try:
    if repo.is_dirty(untracked_files=True):
      repo.git.stash('push', '--include-untracked', '-m', 'xm-snapshot-temp')
      saved_stash = True
      repo.git.stash('apply', '--index')
      repo.git.add(A=True)
      repo.index.commit(
          f'XM Launch: working state snapshot for experiment {experiment_id}',
          skip_hooks=True,
      )
      commit_hash = repo.head.commit.hexsha
    try:
      repo.git.push('origin', f'{commit_hash}:{ref}', '-f', '--no-verify')
    except git.GitCommandError:
      logging.warning(
          'Failed to push repo state for experiment %d: git push failed.',
          experiment_id,
          exc_info=True,
      )
      return RepoStateSnapshot(
          commit_hash=commit_hash, repo_root=repo.working_dir
      )
    commit_url = _get_commit_url(repo, commit_hash)
    if commit_url:
      print(f'Pushed repo state for experiment {experiment_id} to {commit_url}')
    git_url = _get_origin_url(repo)
    if git_url is not None:
      print(
          'To clone the repo at this commit, run `git clone'
          f' --revision={commit_hash} --depth=1 {git_url}`.'
      )
      print(
          'To check out the repo at this commit, run `git fetch origin'
          f' {commit_hash} && git checkout {commit_hash}`'
      )
    else:
      logging.warning(
          'Could not determine origin url to print clone instructions.'
      )
  finally:
    repo.head.reset(original_head, index=True, working_tree=True)
    if original_branch is not None and repo.head.is_detached:
      repo.head.reference = original_branch
    if saved_stash:
      try:
        repo.git.stash('pop', '--index')
      except git.GitCommandError:
        logging.error(
            'Failed to restore local changes after snapshotting for'
            ' experiment %d. Your changes are saved in the git stash.'
            ' Run `git stash pop` to recover them.',
            experiment_id,
        )
  return RepoStateSnapshot(
      commit_hash=commit_hash, commit_url=commit_url, repo_root=repo.working_dir
  )


def _is_interactive_shell(process: psutil.Process) -> bool:
  """Returns whether the process is the shell where user entered command."""
  if process.name() not in _SHELL_NAMES:
    return False
  if len(process.cmdline()) == 1:
    # Shell is running without any args.
    return True
  # Any args should be flags if this is an interactive shell (no script arg
  # like `bash launch.sh`).
  args = process.cmdline()[1:]
  return all([arg.startswith('-') for arg in args])


def find_user_command() -> list[str]:
  """Finds the root launch args of the process that invoked XMC launch.

  Launch scripts are often invoked through an additional bootstrap script.
  To facilitate reproducing of the experiment we try to capture the command
  invoked by the user from terminal.

  Returns:
    List of launch args used to trigger xmanager.
  """
  sid = os.getsid(os.getpid())
  process = psutil.Process(os.getpid())
  parent = process.parent()
  while parent:
    # Pick the child of the UI shell process, or fall back to session leader.
    # The shell process is normally session leader, unless shells are nested.
    if _is_interactive_shell(parent) or parent.pid == sid:
      cmdline = process.cmdline()
      return cmdline
    process, parent = parent, parent.parent()

  return sys.argv


def _get_file_url(
    commit_url: str, launch_script_path: str, repo_root: str
) -> str:
  """Returns a URL pointing to the specific file in the commit."""
  relative_path = os.path.relpath(
      os.path.abspath(launch_script_path), repo_root
  )
  return f'{commit_url}/{relative_path}'


def snapshot_launch_script(
    repo_state: RepoStateSnapshot,
) -> LaunchScriptSnapshot:
  """Finds the launch script path and snapshots its current contents."""
  # We can get the launch script if it's provided explicitly, or when it's run
  # using a Python interpreter (i.e. `xmanager launch <launch_script.py>`).
  try:
    launch_script_path = sys.argv[2]
  except IndexError:
    launch_script_path = ''
  if not launch_script_path.endswith('.py'):
    # The following gets the launch script in the case when we launch an
    # experiment using `blaze run`.
    main_file_path = getattr(sys.modules['__main__'], '__file__', None)
    if (
        main_file_path
        and main_file_path.endswith('.py')
        and os.path.isfile(main_file_path)
        and os.access(main_file_path, os.R_OK)
    ):
      launch_script_path = main_file_path
    else:
      logging.info('Could not locate launch script.')
  try:
    with open(launch_script_path) as f:
      launch_script_content = f.read()
  except OSError:
    logging.info('Could not read launch script.')
    launch_script_content = None
  if repo_state.commit_url is not None and repo_state.repo_root is not None:
    launch_script_url = _get_file_url(
        repo_state.commit_url, launch_script_path, repo_state.repo_root
    )
  else:
    # If we couldn't push the repo state, just use the launch script path as
    # the URL. This will not actually link to anything, but allows us to display
    # the launch script as an artifact.
    launch_script_url = launch_script_path
  return LaunchScriptSnapshot(
      launch_script_path=launch_script_path,
      launch_script_url=launch_script_url,
      launch_script_content=launch_script_content,
  )
