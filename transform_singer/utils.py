
def nested_set(record, target, value):
    """
        Using dot-notation set the value of a dictionary

        Example:

        obj = {
            "foo": {
                "bar": 4
            }
        }

        nested_set(obj, 'foo.bar', 7)
        Returns:
        {
            "foo": {
                "bar": 7
            }
        }

        nested_set(obj, 'foo.zaz', 12)
        Returns:
        {
            "foo": {
                "bar": 7,
                "zaz": 12
            }
        }
    """

    if '.' in target:
        next_level, extra_levels = target.split('.', 1)

        if next_level not in record:
            record[next_level] = {}

        record[next_level] = nested_set(record[next_level], extra_levels, value)
    else:
        record[target] = value

    return record


def nested_get(record: dict, target: str):
    """
        Using dot-notation get the value of a dictionary

        Example:

        obj = {
            "foo": {
                "bar": 4
            }
        }

        nested_get(obj, 'foo.bar')  # returns 4
        nested_get(obj, 'foo.zaz')  # returns None
    """

    if '.' in target:
        next_level, extra_levels = target.split('.', 1)
        return nested_get(record.get(next_level, {}), extra_levels)

    return record.get(target)
