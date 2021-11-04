"""An utility to generate factory methods in packagables.py.

packagables.py has a set of convenience functions which can be mechanically
generated for each ExecutableSpec. But we want them to have all arguments
explicetly written down with proper type annotations. This way IDEs can provide
proper contextual help and autocompletion. We also want make it easy to find
documentation, so having docstrings inplace is important.

We do this in an automated manner to ensure that arguments in ExecutableSpecs
and in the packagables do not diverge and that documentation remains up to date.

Usage: Run this binary and replace relevant parts of packagables.py. Then run
pyformat.
"""

import inspect
import re
from typing import List, Sequence, Type

from absl import app
import inflection
from xmanager.xm import executables
from xmanager.xm import job_blocks

_EXECUTABLES_SPECS = (
    executables.Binary,
    executables.BazelBinary,
    executables.Container,
    executables.BazelContainer,
    executables.PythonContainer,
    executables.Dockerfile,
)

# Reconstructing argument definition from Python introspection is non trivial.
# Partially due to deducing shorter module names (job_blocks rather than
# xmanager.xm.job_blocks). But mostly because type annotations for attr.s
# constructor show field types rather than what converters accept.
# So here we just list how each parameter should be defined.
_KNOWN_ARGS = (
    'base_image: Optional[str] = None',
    'bazel_args: Collection[str] = ()',
    'docker_instructions: Optional[List[str]] = None',
    'dockerfile: Optional[str] = None',
    'entrypoint: Union[executables.ModuleName, executables.CommandList]',
    'dependencies: Collection[executables.BinaryDependency] = ()',
    'image_path: str',
    'label: str',
    'path: str',
    'use_deep_module: bool',
)
_KNOWN_ARGS_DICT = {arg.split(':')[0]: arg for arg in _KNOWN_ARGS}

_ATTRIBUTES_SECTION_HEADER = '  Attributes:'
_ARGS_DOCSTRING = """  Args:
    executor_spec: Where the binary should be launched. Instructs for which
      platform it should be packaged."""
_DOCSTRING_SUFFIX = """
    args: Command line arguments to pass. This can be dict, list or
      xm.SequentialArgs. Dicts are most convenient for keyword flags.
      {'batch_size': 16} is passed as two arguments: --batch_size 16. If
      positional arguments are needed one can use a list or xm.SequentialArgs.
    env_vars: Environment variables to be set.

  Returns:
    A packageable object which can be turned into an executable with
    Experiment.package or Experiment.package_async.
"""


def generate_docstring(executable: Type[job_blocks.ExecutableSpec]) -> str:
  """Returns a docstring for a ExecutableSpec factory method."""
  docstring = executable.__doc__
  if _ATTRIBUTES_SECTION_HEADER not in docstring:
    raise Exception(
        f'Please add Attributes: section to {executable.__name__} docstring.')
  docstring = re.sub(_ATTRIBUTES_SECTION_HEADER, _ARGS_DOCSTRING, docstring)
  docstring = docstring.rstrip() + _DOCSTRING_SUFFIX.rstrip()
  return docstring


def generate_factory_parameters(parameters: List[inspect.Parameter]) -> str:
  """Returns ExecutableSpec factory method parameters definition.

  Args:
    parameters: ExecutableSpec constructor parameters except self.

  Returns:
    Python source code.
  """
  source = '    executor_spec: job_blocks.ExecutorSpec,\n'

  keyword_args_started = False
  for parameter in parameters:
    if (parameter.kind == inspect.Parameter.KEYWORD_ONLY and
        not keyword_args_started):
      keyword_args_started = True
      source += '    *,\n'

    parameter_source = _KNOWN_ARGS_DICT[parameter.name]
    if (parameter.default != inspect.Parameter.empty and
        '=' not in parameter_source):
      parameter_source += f' = {parameter.default!r}'

    source += f'    {parameter_source},\n'

  if not keyword_args_started:
    source += '    *,\n'

  source += '    args: Optional[job_blocks.UserArgs] = None,\n'
  source += '    env_vars: Mapping[str, str] = immutabledict.immutabledict(),\n'
  return source


def generate_factory_method(executable: Type[job_blocks.ExecutableSpec]) -> str:
  """Returns the factory method source code for the given ExecutableSpec."""

  factory_name = inflection.underscore(executable.__name__)
  if factory_name == 'dockerfile':
    # We should rename the Dockerfile class to avoid this naming inconsistency.
    factory_name = 'dockerfile_container'

  signature = inspect.signature(executable.__init__)
  # Skip the `self` parameter.
  parameters = list(signature.parameters.values())[1:]

  executable_args = '\n'.join(
      f'          {p.name}={p.name},' for p in parameters)

  return f'''
def {factory_name}(
{generate_factory_parameters(parameters)}
) -> job_blocks.Packageable:
  # pyformat: disable
  """{generate_docstring(executable)}
  """
  # pyformat: enable
  return job_blocks.Packageable(
      executable_spec=executables.{executable.__name__}(
{executable_args}
      ),
      executor_spec=executor_spec,
      args=args,
      env_vars=env_vars,
  )'''.strip('\n')


def main(argv: Sequence[str]) -> None:
  if len(argv) > 1:
    raise app.UsageError('Too many command-line arguments.')

  for spec in _EXECUTABLES_SPECS:
    print(generate_factory_method(spec))
    print()
    print()


if __name__ == '__main__':
  app.run(main)
