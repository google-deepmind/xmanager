# Copyright 2026 DeepMind Technologies Limited
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

import asyncio
import unittest

from xmanager.xm import framework_utils


class FrameworkLabelTest(unittest.TestCase):

  # Baseline: no labels are active outside any scope.

  def test_default_empty(self):
    self.assertEqual(framework_utils.get_active_framework_labels(), [])

  # Context manager form: `with framework_label('fw'):`.

  def test_context_manager(self):
    with framework_utils.framework_label('fw1'):
      self.assertEqual(framework_utils.get_active_framework_labels(), ['fw1'])
    self.assertEqual(framework_utils.get_active_framework_labels(), [])

  def test_context_manager_nesting(self):
    with framework_utils.framework_label('fw1'):
      self.assertEqual(framework_utils.get_active_framework_labels(), ['fw1'])
      with framework_utils.framework_label('fw2'):
        self.assertEqual(
            framework_utils.get_active_framework_labels(),
            ['fw1', 'fw2'],
        )
      self.assertEqual(framework_utils.get_active_framework_labels(), ['fw1'])
    self.assertEqual(framework_utils.get_active_framework_labels(), [])

  def test_context_manager_is_reusable_across_blocks(self):
    scope = framework_utils.framework_label('fw1')

    with scope:
      self.assertEqual(framework_utils.get_active_framework_labels(), ['fw1'])
    with scope:
      self.assertEqual(framework_utils.get_active_framework_labels(), ['fw1'])
    self.assertEqual(framework_utils.get_active_framework_labels(), [])

  # Decorator form: `@framework_label('fw')`.

  def test_decorator(self):

    @framework_utils.framework_label('fw1')
    def labeled():
      return framework_utils.get_active_framework_labels()

    self.assertEqual(labeled(), ['fw1'])
    self.assertEqual(framework_utils.get_active_framework_labels(), [])

  def test_decorator_on_method(self):
    test_case = self

    class Launcher:

      @framework_utils.framework_label('fw1')
      def launch(self):
        test_case.assertEqual(
            framework_utils.get_active_framework_labels(), ['fw1']
        )

    Launcher().launch()
    self.assertEqual(framework_utils.get_active_framework_labels(), [])

  def test_decorator_is_reusable_across_calls(self):

    @framework_utils.framework_label('fw1')
    def labeled():
      return framework_utils.get_active_framework_labels()

    self.assertEqual(labeled(), ['fw1'])
    self.assertEqual(labeled(), ['fw1'])
    self.assertEqual(framework_utils.get_active_framework_labels(), [])

  def test_decorator_stacks_with_context_manager(self):

    @framework_utils.framework_label('fw2')
    def inner():
      return framework_utils.get_active_framework_labels()

    with framework_utils.framework_label('fw1'):
      self.assertEqual(inner(), ['fw1', 'fw2'])
      self.assertEqual(framework_utils.get_active_framework_labels(), ['fw1'])

  def test_decorator_preserves_function_metadata(self):

    @framework_utils.framework_label('fw1')
    def labeled():
      """Docstring."""

    self.assertEqual(labeled.__name__, 'labeled')
    self.assertEqual(labeled.__doc__, 'Docstring.')

  # Scope unwinding: the label stack is restored on every exit path.

  def test_reentering_same_label_is_a_noop(self):
    with framework_utils.framework_label('fw1'):
      with framework_utils.framework_label('fw1'):
        self.assertEqual(framework_utils.get_active_framework_labels(), ['fw1'])
      self.assertEqual(framework_utils.get_active_framework_labels(), ['fw1'])
    self.assertEqual(framework_utils.get_active_framework_labels(), [])

  def test_recursive_decorated_function(self):

    @framework_utils.framework_label('fw1')
    def recurse(depth):
      if depth:
        return recurse(depth - 1)
      return framework_utils.get_active_framework_labels()

    self.assertEqual(recurse(3), ['fw1'])
    self.assertEqual(framework_utils.get_active_framework_labels(), [])

  def test_label_is_popped_on_exception(self):
    with self.assertRaises(RuntimeError):
      with framework_utils.framework_label('fw1'):
        raise RuntimeError('boom')
    self.assertEqual(framework_utils.get_active_framework_labels(), [])

  # Inputs that framework_label rejects.

  def test_empty_label_raises_value_error(self):
    with self.assertRaises(ValueError):
      framework_utils.framework_label('')

  def test_invalid_label_characters_raise_value_error(self):
    invalid_labels = (
        'my framework',  # Whitespace.
        'my\tframework',
        'my\nframework',
        'my_framework\n',  # Trailing newline, which `re.match` would accept.
        'segment:my_framework',  # `:` delimits key from value downstream.
        "my_'framework",  # Would terminate the quoted config string.
        'my.framework',
        'my/framework',
        'my,framework',
        'my+framework',
        'ml_framework_ünicode',
    )
    for label in invalid_labels:
      with self.subTest(label=label):
        with self.assertRaises(ValueError):
          framework_utils.framework_label(label)

  def test_valid_label_characters_are_accepted(self):
    valid_labels = ('flax', 'T5X', 'my_framework', 'my-framework', 'jax2', 'x')

    for label in valid_labels:
      with self.subTest(label=label):
        with framework_utils.framework_label(label):
          self.assertEqual(
              framework_utils.get_active_framework_labels(), [label]
          )

  def test_decorating_generator_function_raises_type_error(self):
    with self.assertRaises(TypeError):

      @framework_utils.framework_label('fw1')
      def generator():
        yield

  def test_decorating_async_generator_function_raises_type_error(self):
    with self.assertRaises(TypeError):

      @framework_utils.framework_label('fw1')
      async def async_generator():
        yield


class AsyncFrameworkLabelTest(unittest.TestCase):

  # Decorator form on `async def`.

  def test_decorator_on_async_function(self):

    @framework_utils.framework_label('fw1')
    async def labeled():
      return framework_utils.get_active_framework_labels()

    self.assertTrue(asyncio.iscoroutinefunction(labeled))
    self.assertEqual(asyncio.run(labeled()), ['fw1'])
    self.assertEqual(framework_utils.get_active_framework_labels(), [])

  def test_decorator_on_async_function_nesting(self):

    @framework_utils.framework_label('fw2')
    async def inner():
      return framework_utils.get_active_framework_labels()

    @framework_utils.framework_label('fw1')
    async def outer():
      return await inner()

    self.assertEqual(asyncio.run(outer()), ['fw1', 'fw2'])

  def test_recursive_decorated_async_function(self):

    @framework_utils.framework_label('fw1')
    async def recurse(depth):
      if depth:
        return await recurse(depth - 1)
      return framework_utils.get_active_framework_labels()

    self.assertEqual(asyncio.run(recurse(3)), ['fw1'])

  # Concurrency: coroutines must not observe each other's labels.

  def test_concurrent_async_calls_do_not_share_labels(self):

    @framework_utils.framework_label('fw1')
    async def first():
      # Yield to the event loop so that both coroutines are in flight while
      # their labels are set.
      await asyncio.sleep(0)
      return framework_utils.get_active_framework_labels()

    @framework_utils.framework_label('fw2')
    async def second():
      await asyncio.sleep(0)
      return framework_utils.get_active_framework_labels()

    async def run_both():
      return await asyncio.gather(first(), second())

    self.assertEqual(asyncio.run(run_both()), [['fw1'], ['fw2']])

  # Scope unwinding.

  def test_label_is_popped_on_async_exception(self):

    @framework_utils.framework_label('fw1')
    async def boom():
      raise RuntimeError('boom')

    with self.assertRaises(RuntimeError):
      asyncio.run(boom())
    self.assertEqual(framework_utils.get_active_framework_labels(), [])


if __name__ == '__main__':
  unittest.main()
