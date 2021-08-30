import unittest

from transform_singer.utils import nested_get

class TestNestedGet(unittest.TestCase):

    def test_exists(self):
        obj = {
            "foo": {
                "bar": {
                    "biz": True
                }
            }
        }
        self.assertTrue(nested_get(obj, 'foo.bar.biz'))

    def test_notexists(self):
        obj = {
            "foo": {
                "bar": {
                }
            }
        }
        self.assertIsNone(nested_get(obj, 'foo.bar.biz'))
