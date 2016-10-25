import unittest
from operator import not_

from loadsbroker.util import retry


class TestRetry(unittest.TestCase):

    def test_retry_on_result(self):
        attempts = []

        @retry(attempts=4, on_result=not_)
        def foo():
            attempts.append(None)
            return len(attempts) == 3

        self.assertTrue(foo())
        self.assertEqual(len(attempts), 3)

    def test_retry_on_result_propagate(self):
        attempts = []

        @retry(attempts=3, on_result=not_)
        def foo():
            attempts.append(None)
            return False

        self.assertFalse(foo())
        self.assertEqual(len(attempts), 3)

    def test_retry_on_exception(self):
        attempts = []
        exc = ValueError

        @retry(attempts=2,
               on_exception=lambda e: isinstance(e, ValueError))
        def foo():
            attempts.append(None)
            l = len(attempts)
            if l == 1:
                raise exc
            elif l == 2:
                return "foo"
            assert False

        self.assertEqual(foo(), "foo")
        self.assertEqual(len(attempts), 2)

        attempts.clear()
        exc = ZeroDivisionError
        with self.assertRaises(ZeroDivisionError):
            foo()
        self.assertEqual(len(attempts), 1)

    def test_retry_on_exception_propagate(self):
        attempts = []

        @retry(attempts=4,
               on_exception=lambda e: isinstance(e, ZeroDivisionError))
        def foo():
            attempts.append(None)
            1/0

        with self.assertRaises(ZeroDivisionError):
            foo()
        self.assertEqual(len(attempts), 4)
