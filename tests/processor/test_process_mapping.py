from transform_singer.processor import Processor
import unittest
from unittest.mock import MagicMock

from transform_singer.processor import Processor

class TestProcessMapping(unittest.TestCase):

    def test_record(self):
        args = MagicMock()
        processor = Processor(args)
        mapping = {
            "type": "record",
            "key": "foo"
        }
        record = {
            "foo": "bar"
        }
        self.assertEqual(processor.process_mapping(mapping, record), 'bar')


    def test_config(self):
        args = MagicMock()
        args.config = {
            "meta": {
                "foo": "bar"
            }
        }
        processor = Processor(args)
        mapping = {
            "type": "config",
            "key": "foo"
        }
        record = {
        }
        self.assertEqual(processor.process_mapping(mapping, record), 'bar')

    def test_text(self):
        args = MagicMock()
        processor = Processor(args)
        mapping = {
            "type": "text",
            "val": "foo"
        }
        record = {
        }
        self.assertEqual(processor.process_mapping(mapping, record), 'foo')

    def test_join(self):
        args = MagicMock()
        processor = Processor(args)
        mapping = {
            "type": "join",
            "pieces": [
                {
                    "type": "text",
                    "val": "foo"
                },
                {
                    "type": "text",
                    "val": "bar"
                },
            ]
        }
        record = {}
        self.assertEqual(processor.process_mapping(mapping, record), 'foobar')

    def test_coalese(self):
        args = MagicMock()
        processor = Processor(args)
        mapping = {
            "type": "coalesce",
            "objects": [
                {
                    "type": "record",
                    "key": "foo"
                },
                {
                    "type": "record",
                    "key": "bar"
                },
            ]
        }
        record = {
            "bar": "baz"
        }
        self.assertEqual(processor.process_mapping(mapping, record), 'baz')
