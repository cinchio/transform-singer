import singer
import copy
from singer import logger
import json
import hashlib
from transform_singer.utils import nested_get, nested_set


class Processor:
    config = None

    def __init__(self, args):
        self.config = args.config or {}

        if not self.config.get('meta'):
            # We shouldn't end up here but we have it just in case.
            self.config['meta'] = {}

        # Grab the tap_config and add it to the config meta.
        try:
            with open('tap_config.json') as f:
                d = json.load(f)
                self.config['meta'].update(d)
        except Exception as e:
            # Unable to parse/load tap_config json.... so we just ignore it
            pass

    def process_schema(self, message):
        # This function isn't implemented yet.
        # It would be used for filtering out only certain fields for processing
        # TODO convert schema based on mappings
        for key in self.config["mappings"].keys():
            # Loop through all of our mappings to see if we find a mapping that is a subset of the current mapping
            if key.startswith(message["stream"] + "."):
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
        if obj["operator"] == "and":
            return all(
                self._process_condition(condition, record)
                for condition in obj["conditions"]
            )
        if obj["operator"] == "or":
            return any(
                self._process_condition(condition, record)
                for condition in obj["conditions"]
            )

        left = self.process_mapping(obj.get("left"), record)
        right = self.process_mapping(obj.get("right"), record)

        if obj["operator"] == "eq":
            return left == right

        if left is None or right is None:
            return False

        if obj["operator"] == "lt":
            return left < right
        if obj["operator"] == "lte":
            return left <= right
        if obj["operator"] == "gt":
            return left > right
        if obj["operator"] == "gte":
            return left >= right

        return False

    def process_mapping(self, mapping, record):
        try:
            if not mapping:
                return
            elif mapping["type"] == "record":
                # Grab a potentially nested value from the record object
                return nested_get(record, mapping["key"])
            elif mapping["type"] == "config":
                # Grab a potentially nested value from the config meta object
                return nested_get(self.config["meta"], mapping["key"])
            elif mapping["type"] == "text":
                # Set a hard coded text value
                return mapping["val"]
            elif mapping["type"] == "float":
                # Set a hard coded text value
                try:
                    return float(mapping["val"] or 0)
                except:
                    return None
            elif mapping["type"] == "join":
                # Join all of the "pieces" after they are processed through this function
                return "".join(
                    [
                        str(self.process_mapping(mapping, record))
                        for mapping in mapping["pieces"]
                    ]
                )
            elif mapping["type"] == "coalesce":
                # Find the first processed value this exists and use that.
                return next(
                    (
                        self.process_mapping(mapping, record)
                        for mapping in mapping["objects"]
                        if self.process_mapping(mapping, record) or self.process_mapping(mapping, record) == 0
                    ),
                    None,
                )
            elif mapping["type"] == "substr":
                # Return the first `length` number of characters of a string
                return self.process_mapping(mapping["object"], record)[
                    mapping.get("start", 0) : mapping["length"]
                ]
            elif mapping["type"] == "hash":
                # Return a hashed string
                return hashlib.md5(
                    self.process_mapping(mapping["object"], record).encode("utf-8")
                ).hexdigest()
            elif mapping["type"] == "tofloat":
                try:
                    return float(self.process_mapping(mapping["object"], record) or 0)
                except:
                    return None
            elif mapping["type"] == "sum":
                # Add all of the "objects" together
                sum = 0

                for obj in mapping["objects"]:
                    try:
                        sum += float(self.process_mapping(obj, record) or 0)
                    except ValueError:
                        pass

                return sum
            elif mapping["type"] == "multiply":
                # Multiply all of the "objects" together
                product = 1

                for obj in mapping["objects"]:
                    try:
                        product *= float(self.process_mapping(obj, record) or 0)
                    except ValueError:
                        pass

                return product
            elif mapping["type"] == "divide":
                # Start with the first of the "objects" and divide all of the other "objects" from it
                is_first = True
                quotient = 0

                for obj in mapping["objects"]:
                    try:
                        if is_first:
                            quotient = float(self.process_mapping(obj, record) or 0)
                        else:
                            quotient *= float(self.process_mapping(obj, record) or 0)
                    except ValueError:
                        pass

                    is_first = False

                return quotient
            elif mapping["type"] == "difference":
                # Start with the first of the "objects" and subtract all of the other "objects" from it
                is_first = True
                diff = 0

                for obj in mapping["objects"]:
                    try:
                        if is_first:
                            diff = float(self.process_mapping(obj, record) or 0)
                        else:
                            diff -= float(self.process_mapping(obj, record) or 0)
                    except ValueError:
                        pass

                    is_first = False

                return diff
            elif mapping["type"] == "if":
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
                if self._process_condition(mapping.get("condition"), record):
                    # If the condition passed, then return the "then" value
                    return self.process_mapping(mapping.get("then"), record)

                # Condition didn't pass, send the "else" value
                return self.process_mapping(mapping.get("else"), record)
        except:
            logger.log_info(f"Unable to run mapping {mapping} for record: {record}")

    def process_record(self, stream, record, root=None):
        root = root if root else record
        if stream in self.config["mappings"]:
            for mapping in self.config["mappings"][stream]:
                # Loop through the mappings of this stream and process the record.
                mapped_record = {}

                if "exclude" in mapping and bool(
                    self.process_mapping(mapping["exclude"], record)
                ):
                    # Skip this record because of the config
                    continue

                for target, conf in mapping["properties"].items():
                    # Loop through each mapping item and set the the value on the record
                    value = self.process_mapping(conf, record)

                    if value == '':
                        # Ignore empty strings... but not NULL?
                        continue

                    if isinstance(value, str):
                        value = value.replace('\u0000', '')

                    mapped_record = nested_set(
                        mapped_record, target, value
                    )

                # Add the record to the queue for posting to API later
                singer.write_record(mapping["stream"], mapped_record)

        nested_sources = set(
            [
                key
                for key in self.config["mappings"].keys()
                if key.startswith(stream + ".")
            ]
        )

        for key in nested_sources:
            next_level = key[len(stream + ".") :].split(".")[0]
            next_stream = f"{stream}.{next_level}"
            items = nested_get(record, next_level)

            if isinstance(items, list):
                for i in range(len(items)):
                    if type(items[i]) is not dict:
                        item = {"@item": items[i]}
                    else:
                        item = copy.deepcopy(items[i])
                        item["@item"] = item

                    item["@parent"] = record
                    item["@root"] = root
                    item["@index"] = i

                    self.process_record(next_stream, item, root)
            elif items:
                item = copy.deepcopy(items)
                try:
                    item["@parent"] = record
                    item["@root"] = root
                except:
                    logger.log_info(f"Error trying to set parent/root {item}")
                self.process_record(next_stream, item, root)

    def process_state(self, message):
        # Forward state along
        singer.write_state(message["value"])

    def process_log(self, message):
        if message.get('event') == 'START':
            # Augement data and pass along
            message['company'] = self.config['meta'].get('company_id')
            message['credential'] = self.config['meta'].get('credential_id')

        logger.log_info(json.dumps(message))

    def process(self, message):
        if message["type"] == "SCHEMA":
            self.process_schema(message)
        elif message["type"] == "RECORD":
            self.process_record(message["stream"], message["record"])
        elif message["type"] == "STATE":
            self.process_state(message)
