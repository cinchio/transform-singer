import unittest

from transform_singer.utils import nested_set

class TestNestedSet(unittest.TestCase):

    def test_exists(self):
        obj = {
            "foo": {
                "bar": "zip"
            }
        }
        obj = nested_set(obj, 'foo.bar', 'baz')
        self.assertDictEqual(obj, {
            "foo": {
                "bar": "baz"
            }
        })

    def test_not_exists(self):
        obj = {}
        obj = nested_set(obj, 'foo.bar', 'baz')
        self.assertDictEqual(obj, {
            "foo": {
                "bar": "baz"
            }
        })

    def test_not_top_level(self):
        obj = {}
        obj = nested_set(obj, 'foo', 'bar')
        self.assertDictEqual(obj, {
            "foo": "bar"
        })

    def test_overwrite(self):
        obj = {
            "foo": {
                "bar": {
                    "zap": "test"
                }
            }
        }
        obj = nested_set(obj, 'foo.bar', 'baz')
        self.assertDictEqual(obj, {
            "foo": {
                "bar": "baz"
            }
        })
