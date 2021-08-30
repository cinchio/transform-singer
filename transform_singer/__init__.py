import io
import json
import sys

import singer
from transform_singer.processor import Processor

LOGGER = singer.get_logger()
REQUIRED_CONFIG_KEYS = ['mappings']


@singer.utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    processor = Processor(args)

    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    for message in input_messages:
        message = json.loads(message)
        processor.process(message)


if __name__ == "__main__":
    main()
