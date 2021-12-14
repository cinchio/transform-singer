import unittest
from unittest.mock import MagicMock, patch

from transform_singer.processor import Processor


class TestProcessMapping(unittest.TestCase):
    @patch("transform_singer.processor.singer.write_record")
    def test_simple_stream(self, write_record):
        args = MagicMock()
        args.config = {
            "mappings": {
                "facilities": [
                    {
                        "stream": "location",
                        "properties": {
                            "name": {"type": "record", "key": "name"},
                        },
                    }
                ],
            }
        }
        processor = Processor(args)
        processor.process_record("facilities", {"name": "Foo"})

        write_record.assert_called_once_with("location", {"name": "Foo"})

    @patch("transform_singer.processor.singer.write_record")
    def test_nested_child_stream(self, write_record):
        args = MagicMock()
        args.config = {
            "mappings": {
                "facilities.children": [
                    {
                        "stream": "location",
                        "properties": {
                            "name": {"type": "record", "key": "name"},
                        },
                    }
                ],
            }
        }
        processor = Processor(args)
        processor.process_record(
            "facilities",
            {
                "children": [
                    {"name": "Joe"},
                    {"name": "Bob"},
                    {"name": "Ann"},
                ]
            },
        )

        self.assertEqual(len(write_record.call_args_list), 3)

        write_record.assert_any_call("location", {"name": "Joe"})
        write_record.assert_any_call("location", {"name": "Bob"})
        write_record.assert_any_call("location", {"name": "Ann"})

    @patch("transform_singer.processor.singer.write_record")
    def test_nested_grandchild_stream(self, write_record):
        args = MagicMock()
        args.config = {
            "mappings": {
                "facilities.children.grandchildren": [
                    {
                        "stream": "location",
                        "properties": {
                            "name": {"type": "record", "key": "name"},
                            "parent": {"type": "record", "key": "@parent.name"},
                            "grandparent": {
                                "type": "record",
                                "key": "@parent.@parent.name",
                            },
                            "rootparent": {"type": "record", "key": "@root.name"},
                        },
                    }
                ],
            }
        }
        processor = Processor(args)
        processor.process_record(
            "facilities",
            {
                "name": "Jared",
                "children": {
                    "name": "Mary",
                    "grandchildren": [
                        {"name": "Joe"},
                        {"name": "Bob"},
                        {"name": "Ann"},
                    ],
                },
            },
        )

        self.assertEqual(len(write_record.call_args_list), 3)

        write_record.assert_any_call(
            "location",
            {
                "name": "Joe",
                "parent": "Mary",
                "grandparent": "Jared",
                "rootparent": "Jared",
            },
        )
        write_record.assert_any_call(
            "location",
            {
                "name": "Bob",
                "parent": "Mary",
                "grandparent": "Jared",
                "rootparent": "Jared",
            },
        )
        write_record.assert_any_call(
            "location",
            {
                "name": "Ann",
                "parent": "Mary",
                "grandparent": "Jared",
                "rootparent": "Jared",
            },
        )

    @patch("transform_singer.processor.singer.write_record")
    def test_extreme_nested_stream(self, write_record):
        args = MagicMock()
        args.config = {
            "mappings": {
                "facilities.children.children": [
                    {
                        "stream": "location",
                        "properties": {
                            "name": {"type": "record", "key": "name"},
                            "parent": {"type": "record", "key": "@parent.name"},
                            "grandparent": {
                                "type": "record",
                                "key": "@parent.@parent.name",
                            },
                            "rootparent": {"type": "record", "key": "@root.name"},
                        },
                    }
                ],
            }
        }
        processor = Processor(args)
        processor.process_record(
            "facilities",
            {
                "name": "Grandperson",
                "spouse": "Grandspouse",
                "children": [
                    {
                        "name": "Parent1",
                        "spouse": "Spouse1",
                        "children": [
                            {"name": "Child1A"},
                            {"name": "Child1B"},
                        ],
                    },
                    {
                        "name": "Parent2",
                        "spouse": "Spouse2",
                        "children": [
                            {"name": "Child2A"},
                            {"name": "Child2B"},
                        ],
                    },
                ],
            },
        )

        self.assertEqual(len(write_record.call_args_list), 4)

        write_record.assert_any_call(
            "location",
            {
                "name": "Child1A",
                "parent": "Parent1",
                "grandparent": "Grandperson",
                "rootparent": "Grandperson",
            },
        )
        write_record.assert_any_call(
            "location",
            {
                "name": "Child1B",
                "parent": "Parent1",
                "grandparent": "Grandperson",
                "rootparent": "Grandperson",
            },
        )
        write_record.assert_any_call(
            "location",
            {
                "name": "Child2A",
                "parent": "Parent2",
                "grandparent": "Grandperson",
                "rootparent": "Grandperson",
            },
        )
        write_record.assert_any_call(
            "location",
            {
                "name": "Child2B",
                "parent": "Parent2",
                "grandparent": "Grandperson",
                "rootparent": "Grandperson",
            },
        )
