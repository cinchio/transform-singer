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

    nested_set(obj, 'fiz[0].zaz', 12)
    Returns:
    {
        "foo": {
            "bar": 7,
            "zaz": 12
        },
        "fix": [
            {
                "zaz": 12
            }
        ]
    }

    nested_set(obj, 'fiz[0].zap', 2)
    Returns:
    {
        "foo": {
            "bar": 7,
            "zaz": 12
        },
        "fix": [
            {
                "zaz": 12,
                "zap": 2
            }
        ]
    }

    nested_set(obj, 'fom[0]', 2)
    Returns:
    {
        "foo": {
            "bar": 7,
            "zaz": 12
        },
        "fix": [
            {
                "zaz": 12,
                "zap": 2
            }
        ],
        "fom": [
            2
        ]
    }
    """

    if "." in target:
        next_level, extra_levels = target.split(".", 1)

        if "[" in next_level:
            next_level, index = next_level.split("[")
            index = index[:-1]

            if next_level not in record:
                record[next_level] = []

            if not index:
                new_dict = dict()
                record[next_level].append(new_dict)
                nested_set(new_dict, extra_levels, value)
            else:
                # Add item to this spot in the array
                index = int(index)
                if index > len(record[next_level]):
                    record[next_level] += (1 + index - len(record[next_level])) * [None]

                if not record[next_level][index]:
                    record[next_level][index] = {}

                nested_set(record[next_level][index], extra_levels, value)
        else:
            if next_level not in record:
                record[next_level] = {}

            record[next_level] = nested_set(record[next_level], extra_levels, value)
    elif "[" in target:
        target, index = target.split("[")
        index = index[:-1]

        if target not in record:
            record[target] = []

        if not index:
            # Add item to the end of the array
            record[target].append(value)
        else:
            # Add item to this spot in the array
            index = int(index)
            if index > len(record[target]):
                record[target] += (1 + index - len(record[target])) * [None]
            record[target][index] = value
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

    if "." in target:
        next_level, extra_levels = target.split(".", 1)
        return nested_get(record.get(next_level, {}), extra_levels)

    return record.get(target)
