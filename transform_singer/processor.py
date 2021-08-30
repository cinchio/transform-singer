import singer
from transform_singer.utils import nested_get, nested_set

class Processor:
    config = None

    def __init__(self, args):
        self.config = args.config

    def process_schema(self, message):
        # TODO convert schema based on mappings
        for key in self.config['mappings'].keys():
            # Loop through all of our mappings to see if we find a mapping that is a subset of the current mapping
            if key.startswith(message['stream'] + '.'):
                # Convert this schema
                pass

    def process_mapping(self, mapping, record):
        if mapping['type'] == 'record':
            # Grab a potentially nested value from the record object
            return nested_get(record, mapping['key'])
        elif mapping['type'] == 'config':
            # Grab a potentially nested value from the config meta object
            return nested_get(self.config['meta'], mapping['key'])
        elif mapping['type'] == 'text':
            # Set a hard coded text value
            return mapping['val']
        elif mapping['type'] == 'join':
            # Join all of the "pieces" after they are processed through this function
            return ''.join([self.process_mapping(mapping, record) for mapping in mapping['pieces']])
        elif mapping['type'] == 'coalesce':
            # Find the first processed value this exists and use that.
            return next((self.process_mapping(mapping, record) for mapping in mapping['objects'] if self.process_mapping(mapping, record)), None)

    def process_record(self, message):
        for mapping in self.config['mappings'][message['stream']]:
            # Loop through the mappings of this stream and process the record.
            record = {}

            for target, conf in mapping['properties'].items():
                # Loop through each mapping item and set the the value on the record
                record = nested_set(record, target, self.process_mapping(conf, message['record']))

            # Add the record to the queue for posting to API later
            singer.write_record(mapping['stream'], record)

        # Search for nested sources
        for key in self.config['mappings'].keys():
            # Loop through all of our mappings to see if we find a mapping that is a subset of the current mapping
            if key.startswith(message['stream'] + '.'):
                next_level = ''
                items = None

                for part in key[len(message['stream'] + '.'):].split('.'):
                    # Break the mapping apart by dot-notation to find the next level that is a list
                    next_level += '.' + part if next_level else part
                    items = nested_get(message['record'], next_level)

                    if isinstance(items, list):
                        # We found the list so let's process those items
                        break

                if not isinstance(items, list):
                    # We actually didn't find a list so let's move on to the next mapping
                    continue

                for item in items:
                    # Loop through the sub items of this record and process them.
                    record = {**item, '@parent': message['record']}

                    self.process_record({"stream": message['stream'] + '.' + next_level, "record": record})


    def process_state(self, message):
        singer.write_state(message['value'])

    def process(self, message):
        if message['type'] == 'SCHEMA':
            self.process_schema(message)
        elif message['type'] == 'RECORD':
            self.process_record(message)
        elif message['type'] == 'STATE':
            self.process_state(message)
