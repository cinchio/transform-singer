import singer
from singer import logger
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

    def _process_condition(self, obj, record):
        """
        Example:
        Compares the record's first_name with the text value of "Chris"
        {
            "operator": "eq",
            "left": {
                "type": "record",
                "key": "first_name",
            },
            "right": {
                "type": "text",
                "val": "Chris"
            }
        }
        """
        if obj['operator'] == 'and':
            return all(self._process_condition(condition, record) for condition in obj['conditions'])
        if obj['operator'] == 'or':
            return any(self._process_condition(condition, record) for condition in obj['conditions'])

        left = self.process_mapping(obj.get('left'), record)
        right = self.process_mapping(obj.get('right'), record)

        if obj['operator'] == 'eq':
            return left == right
        if obj['operator'] == 'lt':
            return left < right
        if obj['operator'] == 'lte':
            return left <= right
        if obj['operator'] == 'gt':
            return left > right
        if obj['operator'] == 'gte':
            return left >= right

        return False

    def process_mapping(self, mapping, record):
        if not mapping:
            return
        elif mapping['type'] == 'record':
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
            return ''.join([str(self.process_mapping(mapping, record)) for mapping in mapping['pieces']])
        elif mapping['type'] == 'coalesce':
            # Find the first processed value this exists and use that.
            return next((self.process_mapping(mapping, record) for mapping in mapping['objects'] if self.process_mapping(mapping, record)), None)
        elif mapping['type'] == 'substr':
            # Find the first processed value this exists and use that.
            return self.process_mapping(mapping['object'], record)[:mapping['length']]
        elif mapping['type'] == 'sum':
            # Add all of the "objects" together
            sum = 0

            for obj in mapping['objects']:
                try:
                    sum += float(self.process_mapping(obj, record))
                except ValueError:
                    pass

            return sum
        elif mapping['type'] == 'difference':
            # Start with the first of the "objects" and subtract all of the other "objects" from it
            is_first = True
            diff = 0

            for obj in mapping['objects']:
                try:
                    if is_first:
                        diff = float(self.process_mapping(obj, record))
                    else:
                        diff -= float(self.process_mapping(obj, record))
                except ValueError:
                    pass

                is_first = False

            return diff
        elif mapping['type'] == 'difference':
            # Find the first processed value this exists and use that.
            return self.process_mapping(mapping['object'], record)[:mapping['length']]
        elif mapping['type'] == 'if':
            """
            Run a condition on the mapping.   This particularly useless mapping will change the first_name if it's Chris to Christopher.
            Otherwise it will return the original first_name.  For nested if statements you can add another "if" type in the "else" or the
            "then".

            Example:
            {
                "type": "if",
                "condition": {
                    "operator": "eq",
                    "left": {
                        "type": "record",
                        "key": "first_name",
                    },
                    "right": {
                        "type": "text",
                        "val": "Chris"
                    }
                }
                "then": {
                    "type": "text",
                    "val": "Christopher"
                },
                "else": {
                    "type": "record",
                    "key": "first_name",
                }
            }
            """
            if self._process_condition(mapping.get('condition'), record):
                # If the condition passed, then return the "then" value
                return self.process_mapping(mapping.get('then'), record)

            # Condition didn't pass, send the "else" value
            return self.process_mapping(mapping.get('else'), record)

    def process_record(self, message):
        if message['stream'] not in self.config['mappings']:
            # Do we want to pass no the record?
            # singer.write_record(message['stream'], message['record'])
            return

        for mapping in self.config['mappings'][message['stream']]:
            # Loop through the mappings of this stream and process the record.
            record = {}

            if 'exclude' in mapping and bool(self.process_mapping(mapping['exclude'], message['record'])):
                # Skip this record because of the config
                continue

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

                for i in range(len(items)):
                    item = items[i]

                    # Loop through the sub items of this record and process them.
                    record = {**item, '@parent': message['record'], '@index': i}

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
