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

from typing import List
import unittest

from xmanager.xm import pattern_matching
from xmanager.xm import utils


class UtilsTest(unittest.TestCase):

  def testMatch_selectsAppropriateBranch(self):
    matcher = pattern_matching.match(
        pattern_matching.Case([int], lambda _: 'A'),
        pattern_matching.Case([str], lambda _: 'B'),
    )

    self.assertEqual(matcher('abc'), 'B')

  def testMatch_throwsWhenNoneFit(self):
    matcher = pattern_matching.match()

    with self.assertRaises(TypeError):
      matcher(0)

  def testMatch_earlierOptionWins(self):

    class P:
      pass

    class C(P):
      pass

    matcher = pattern_matching.match(
        pattern_matching.Case([P], lambda _: 'P'),
        pattern_matching.Case([C], lambda _: 'C'),
    )

    self.assertEqual(matcher(C()), 'P')

  def testMatch_asVisitor(self):

    def visit_int(n: int):
      return n * 2

    def visit_str(s: str):
      return len(s)

    matcher = pattern_matching.match(visit_int, visit_str)

    self.assertEqual(matcher(1), 2)
    self.assertEqual(matcher('zzz'), 3)

  def testMatch_multipleArguments(self):

    def visit_just_one(k: int):
      return k * 2

    def visit_too_many(k: int, l: int, m: int):
      return k * l * m

    def visit_just_right(k: int, l: int):
      return k * l

    matcher = pattern_matching.match(visit_just_one, visit_too_many,
                                     visit_just_right)

    self.assertEqual(matcher(2, 3), 6)

  def testMatch_parameterizedGenerics(self):
    """Test that generics parameters don't confuse isinstance.

    Note that the parameters don't participate in matching. We can't distinguish
    List[int] and List[str].
    """

    def first_element(l: List[int]) -> int:
      return l[0]

    self.assertEqual(pattern_matching.match(first_element)([1, 2]), 1)

  def testMatch_missingAnnotation(self):
    with self.assertRaises(ValueError):
      pattern_matching.match(lambda x: x)

  @utils.run_in_asyncio_loop
  async def testMatch_async(self):

    async def visit_int(n: int):
      return n * 2

    async def visit_str(s: str):
      return len(s)

    matcher = pattern_matching.async_match(visit_int, visit_str)

    self.assertEqual(await matcher(1), 2)
    self.assertEqual(await matcher('zzz'), 3)


if __name__ == '__main__':
  unittest.main()
