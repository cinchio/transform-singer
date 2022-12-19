import unittest

from transform_singer.utils import nested_set


class TestNestedSet(unittest.TestCase):
    def test_exists(self):
        obj = {"foo": {"bar": "zip"}}
        obj = nested_set(obj, "foo.bar", "baz")
        self.assertDictEqual(obj, {"foo": {"bar": "baz"}})

    def test_not_exists(self):
        obj = {}
        obj = nested_set(obj, "foo.bar", "baz")
        self.assertDictEqual(obj, {"foo": {"bar": "baz"}})

    def test_not_top_level(self):
        obj = {}
        obj = nested_set(obj, "foo", "bar")
        self.assertDictEqual(obj, {"foo": "bar"})

    def test_overwrite(self):
        obj = {"foo": {"bar": {"zap": "test"}}}
        obj = nested_set(obj, "foo.bar", "baz")
        self.assertDictEqual(obj, {"foo": {"bar": "baz"}})

    def test_array(self):
        obj = {}
        obj = nested_set(obj, "foo[]", "baz")
        self.assertDictEqual(obj, {"foo": ["baz"]})
        obj = nested_set(obj, "foo[]", "bar")
        self.assertDictEqual(obj, {"foo": ["baz", "bar"]})

    def test_array2(self):
        obj = {}
        obj = nested_set(obj, "foo[1]", "baz")
        self.assertDictEqual(obj, {"foo": [None, "baz"]})
        obj = nested_set(obj, "foo[1]", "bar")
        self.assertDictEqual(obj, {"foo": [None, "bar"]})

    def test_array_dict(self):
        obj = {}
        obj = nested_set(obj, "foo[].fiz", "baz")
        self.assertDictEqual(obj, {"foo": [{"fiz": "baz"}]})
        obj = nested_set(obj, "foo[].fiz", "bar")
        self.assertDictEqual(obj, {"foo": [{"fiz": "baz"}, {"fiz": "bar"}]})

    def test_array_dict2(self):
        obj = {}
        obj = nested_set(obj, "foo[1].fiz", "baz")
        self.assertDictEqual(obj, {"foo": [None, {"fiz": "baz"}]})
        obj = nested_set(obj, "foo[1].fiz", "bar")
        self.assertDictEqual(obj, {"foo": [None, {"fiz": "bar"}]})
