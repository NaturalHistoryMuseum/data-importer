from typing import Tuple


def read_emu_extract(text: str) -> Tuple[str, dict]:
    data = {}
    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue

        # the format is <field>:<index>=<value>
        field, value = line.split("=", 1)
        field = field.split(":", 1)[0]

        existing = data.get(field)
        if existing is None:
            # the value isn't in the data dict, add it
            data[field] = value
        else:
            if isinstance(existing, tuple):
                # there is an existing set of values in the data dict, add
                # the new value in a new tuple
                data[field] = (*existing, value)
            else:
                # there is an existing value (just one) in the data dict,
                # add the new value in a new tuple
                data[field] = (existing, value)

    return data["irn"], data
