import csv
import re
import string
from collections import OrderedDict
from decimal import Decimal

import unidecode

from witchcraft.combinators import execute, query, template
from witchcraft.dateutil.parser import parse as dateutil_parse

prefix_dict = {
    "pgsql": "psql",
    "mysql": "mysql",
    "oracle": "oracle",
    "mssql": "mssql",
}


def get_script_prefix(connection):
    if hasattr(connection, "database_type"):
        return prefix_dict.get(connection.database_type)
    else:
        return prefix_dict.get("pgsql")


def find_keys(dps):
    keys = set()

    for i in dps:
        keys |= set(i.keys())

    return keys


# TODO: get rid of side effect
def remove_metadata(data_points):
    for item in data_points:
        if "_updated_at" in item:
            del item["_updated_at"]

        if "_created_at" in item:
            del item["_created_at"]


def create_table(
    connection, schema_name, table_name, fields, primary_keys, version_column=None
):
    primary_keys = list(primary_keys)

    columns = list(fields.items())

    if version_column is not None:
        primary_keys.append(version_column)
        columns.append((version_column, {"psql_type": "bigint"}))

    prefix = get_script_prefix(connection)  # get_script_prefix(connection)
    execute(
        connection,
        template(
            "%s_create_table" % prefix,
            dict(
                columns=columns,
                schema_name=schema_name,
                table_name=table_name,
                primary_keys=primary_keys,
            ),
        ),
    )


# TODO: implement column upgrade/add as we go through data points To make this
# possible DataPoint have to implement simple way of checking if fields sqltype changed.
#


def prepare_table(
    connection,
    schema_name,
    table_name,
    data_points,
    load_table_columns,
    primary_keys,
    version_column=None,
):
    fields = OrderedDict([])

    for dp in data_points:
        fields.update(dp.fields)

    prefix = get_script_prefix(connection)

    required_columns = find_keys(data_points)
    discovery_table_name = table_name

    if prefix == "oracle":
        discovery_table_name = table_name.upper()

    result = query(
        connection,
        template(
            "%s_discover_columns" % prefix,
            dict(schema_name=schema_name, table_name=discovery_table_name),
        ),
    )

    discovered_columns = set(map(lambda r: r.column_name.lower(), result))

    if len(discovered_columns) == 0:
        create_table(connection, schema_name, table_name, fields, primary_keys)

        result = query(
            connection,
            template(
                "%s_discover_columns" % prefix,
                dict(schema_name=schema_name, table_name=discovery_table_name),
            ),
        )

        discovered_columns = set(map(lambda r: r.column_name.lower(), result))

    else:
        discovered_pkeys = filter(lambda r: r.is_pkey, result)
        discovered_pkeys = list(map(lambda r: r.column_name.lower(), discovered_pkeys))

        if version_column is not None:
            discovered_pkeys.remove(version_column)

        if len(primary_keys) != 0:
            if set(primary_keys) != set(discovered_pkeys):
                raise ValueError(
                    "Primary keys in destination table are not matching with defined primary keys %s %s"
                    % (set(primary_keys), set(discovered_pkeys))
                )
            primary_keys = discovered_pkeys

    missing_columns = required_columns - discovered_columns

    for column_name in missing_columns:
        column_type = fields.get(column_name)["%s_type" % prefix]

        execute(
            connection,
            template(
                "%s_add_column" % prefix,
                dict(
                    schema_name=schema_name,
                    table_name=table_name,
                    column_name=column_name,
                    column_type=column_type,
                ),
            ),
        )

    if version_column is not None and version_column not in discovered_columns:
        execute(
            connection,
            template(
                "%s_add_column" % prefix,
                dict(
                    schema_name=schema_name,
                    table_name=table_name,
                    column_name=version_column,
                    column_type="bigint",
                ),
            ),
        )

    if load_table_columns is not None:
        missing_columns = required_columns - load_table_columns

        for column_name in missing_columns:
            column_type = fields.get(column_name)["%s_type" % prefix]

            execute(
                connection,
                template(
                    "%s_load_table_add_column" % prefix,
                    dict(column_name=column_name, column_type=column_type),
                ),
            )

            load_table_columns.add(column_name)

    return primary_keys


def upsert_prepare_load_table(connection, batch, primary_keys):
    prefix = get_script_prefix(connection)

    fields = OrderedDict([])

    for dp in batch:
        fields.update(dp.fields)

    execute(
        connection,
        template(
            "%s_prepare_load_table" % prefix,
            dict(columns=fields.items(), primary_keys=primary_keys),
            prefix,
        ),
    )


def upsert_data(connection, schema_name, table_name, data_points, primary_keys):
    prefix = get_script_prefix(connection)
    column_names = list(find_keys(data_points))

    connection.begin()

    execute(
        connection,
        template(
            "%s_upsert_load" % prefix,
            dict(
                schema_name=schema_name,
                table_name=table_name,
                column_names=column_names,
                columns=data_points[0].fields.items(),
                data_points=data_points,
                primary_keys=primary_keys,
            ),
            prefix,
        ),
    )

    updated = execute(
        connection,
        template(
            "%s_upsert_update" % prefix,
            dict(
                schema_name=schema_name,
                table_name=table_name,
                column_names=column_names,
                columns=data_points[0].fields.items(),
                data_points=data_points,
                primary_keys=primary_keys,
            ),
            prefix,
        ),
    )

    inserted = execute(
        connection,
        template(
            "%s_upsert_insert" % prefix,
            dict(
                schema_name=schema_name,
                table_name=table_name,
                column_names=column_names,
                columns=data_points[0].fields.items(),
                data_points=data_points,
                primary_keys=primary_keys,
            ),
            prefix,
        ),
    )

    connection.commit()

    return (inserted, updated)


def insert_data(connection, schema_name, table_name, data_points):
    prefix = get_script_prefix(connection)
    column_names = list(find_keys(data_points))

    tx = connection.begin()

    inserted = query(
        connection,
        template(
            "%s_insert" % prefix,
            dict(
                schema_name=schema_name,
                table_name=table_name,
                column_names=column_names,
                columns=data_points[0].fields.items(),
                data_points=data_points,
            ),
            prefix,
        ),
    )

    if hasattr(connection, "commit"):
        connection.commit()

    else:
        tx.commit()

    return inserted[0].count


def delete_data(connection, schema_name, table_name):
    prefix = get_script_prefix(connection)

    result = query(
        connection,
        template(
            "%s_discover_columns" % prefix,
            dict(schema_name=schema_name, table_name=table_name),
        ),
    )

    if len(result) == 0:
        return 0

    # TODO: use SqlSession.delete directly?
    deleted_count = execute(
        connection,
        template(
            "%s_delete" % prefix,
            dict(schema_name=schema_name, table_name=table_name),
            prefix,
        ),
    )

    return deleted_count


def discover_columns(connection, schema_name, table_name):
    prefix = get_script_prefix(connection)
    result = query(
        connection,
        template(
            "%s_discover_columns" % prefix,
            dict(schema_name=schema_name, table_name=table_name),
        ),
    )

    return result


def get_max_version(connection, schema_name, table_name, version_column="version"):
    prefix = get_script_prefix(connection)

    result = query(
        connection,
        template(
            "%s_discover_columns" % prefix,
            dict(schema_name=schema_name, table_name=table_name),
        ),
    )

    if len(result) == 0:
        return 1

    tpl = template(
        "%s_max_version" % prefix,
        dict(
            schema_name=schema_name,
            table_name=table_name,
            version_column=version_column,
        ),
        connection.database_type,
    )

    result = query(connection, tpl)

    if len(result) > 0 and getattr(result[0], version_column) is not None:
        return getattr(result[0], version_column) + 1
    else:
        return 1


class InputType(object):
    def __init__(self, name, params=None):
        if params is None:
            params = {}

        self.name = name
        self.params = params

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name


def extract_number(number_str, decimal=None):
    # TODO: handle decimal=False/True
    # TODO: regexp \s*[-+]?\s*[€$%]?[0-9,\.]\+
    separators = []
    groups = [""]
    groups_index = 0
    sign = None
    if number_str is None:
        return None

    for c in number_str:
        if c in [".", ","]:
            separators.append(c)
            groups.append("")
            groups_index += 1

        elif c in [" ", "€", "$", "%"]:
            continue

        elif c in ["+", "-"] and sign is None:
            sign = c

        elif c in "1234567890":
            groups[groups_index] += c

            if sign is None:
                sign = "+"
        else:
            return None

    if len(groups) == 0:
        return None

    elif len(groups[0]) == 0:
        if len("".join(groups)) == 0:
            return None

        number = Decimal("%s0.%s" % (sign, "".join(groups)))
        precision = len(number.as_tuple().digits)
        scale = -number.as_tuple().exponent
        input_type = InputType("numeric", dict(precision=precision, scale=scale))
        return number, input_type

    elif len(groups) == 1:
        value = int(sign + groups[0])

        if value <= 2147483647 and value >= -2147483648:
            return value, InputType("int")

        elif value <= 9223372036854775807 and value >= -9223372036854775808:
            return value, InputType("bigint")

        else:
            return value, InputType("text")

    # xx.xx
    elif len(groups) == 2:
        if decimal is not None:
            if decimal == True:
                number = Decimal("%s%s.%s" % (sign, "".join(groups[0:-1]), groups[-1]))
                precision = len(number.as_tuple().digits)
                scale = -number.as_tuple().exponent
                input_type = InputType(
                    "numeric", dict(precision=precision, scale=scale)
                )
                return number, input_type

            else:
                value = int("%s%s" % (sign, "".join(groups)))

                if value <= 2147483647 and value >= -2147483648:
                    return value, InputType("int")

                elif value <= 9223372036854775807 and value >= -9223372036854775808:
                    return value, InputType("bigint")

        elif len(groups[-1]) != 3:
            number = Decimal("%s%s.%s" % (sign, "".join(groups[0:-1]), groups[-1]))
            precision = len(number.as_tuple().digits)
            scale = -number.as_tuple().exponent
            input_type = InputType(
                "numeric", dict(precision=precision, scale=scale)
            )
            return number, input_type

        else:
            value = "%s%s" % (sign, "".join(groups))
            return value, InputType("text")

    # x.xxx,xx / x,xxx.xx
    elif len(groups) == 3 and separators[-1] != separators[-2]:
        number = Decimal("%s%s.%s" % (sign, "".join(groups[0:-1]), groups[-1]))
        precision = len(number.as_tuple().digits)
        scale = -number.as_tuple().exponent
        input_type = InputType("numeric", dict(precision=precision, scale=scale))
        return number, input_type

    elif len(groups) > 3 and separators[-1] != separators[-2]:
        number = Decimal("%s%s.%s" % (sign, "".join(groups[0:-1]), groups[-1]))
        precision = len(number.as_tuple().digits)
        scale = -number.as_tuple().exponent
        input_type = InputType("numeric", dict(precision=precision, scale=scale))
        return number, input_type

    elif len(groups) >= 3 and separators[-1] == separators[-2]:
        value = int("%s%s" % (sign, "".join(groups)))

        if value <= 2147483647 and value >= -2147483648:
            return value, InputType("int")

        elif value <= 9223372036854775807 and value >= -9223372036854775808:
            return value, InputType("bigint")

        else:
            return value, InputType("text")

    else:
        return None


def detect_dayfirst(dates):
    first_test = None
    second_test = None

    try:
        for i in dates:
            dateutil_parse(i, dayfirst=True)

        first_test = True
    except:
        first_test = False

    try:
        for i in dates:
            dateutil_parse(i, dayfirst=False)

        second_test = True
    except:
        second_test = False

    if first_test != second_test:
        return first_test
    else:
        None


def parse_csv(input_data, delimiter=";", quotechar='"'):
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(input_data, delimiters=";\t,")
    except:
        csv.register_dialect("dlb_excel", delimiter=delimiter, quotechar=quotechar)
        dialect = csv.get_dialect("dlb_excel")

    data = input_data
    data = data.splitlines()
    data = csv.reader(data, dialect=dialect)

    # remove redundant new lines
    data = filter(lambda i: len(i) != 0, data)

    return list(data)


def detect_type(value, current_type=None):
    # TODO: Optimize, this is where 80-90% time is spent when loading data
    if value is None:
        return None, current_type

    if value == '' and (current_type is None or current_type.name != 'text'):
        return None, current_type

    if current_type is None:
        extract_result = extract_number(value)

        if extract_result is not None:
            return extract_result

        try:
            result, dayfirst = dateutil_parse(value)
            # TODO: handle timezones
            return result, InputType(
                "timestamp", dict(dayfirst=dayfirst, last_value=result)
            )

        except Exception as e:
            pass

        if value.lower() in ["true", "t", "yes", "y"]:
            return True, InputType("bool")

        elif value.lower() in ["false", "f", "no", "n"]:
            return False, InputType("bool")

        return value, InputType("text")

    elif current_type.name == "text":
        return str(value), InputType("text")

    elif current_type.name == "numeric":
        if value == "" or value is None:
            return None, current_type

        extract_result = extract_number(value, True)

        # not a number
        if extract_result is None:
            return value, InputType("text")

        # it is a number
        new_type = extract_result[1]
        value = extract_result[0]

        if new_type.name == "numeric":
            # enlarge precision to hold new value if necessary

            precision = max(
                current_type.params["precision"], new_type.params["precision"]
            )
            scale = max(current_type.params["scale"], new_type.params["scale"])

            # make sure that it will hold old value
            natural_lenght = max(
                precision - scale,
                current_type.params["precision"] - current_type.params["scale"],
            )
            precision = max(precision, natural_lenght + scale)

        elif new_type.name == "int":
            natural_lenght = len(str(value))
            precision = max(current_type.params["precision"], natural_lenght)
            scale = current_type.params["scale"]
            precision = max(natural_lenght + scale, precision)

        else:
            return value, InputType("text")

        return value, InputType("numeric", dict(precision=precision, scale=scale))

    elif (
        current_type.name == "int"
        or current_type.name == "integer"
        or current_type.name == "bigint"
    ):
        if value == "" or value is None:
            return None, current_type

        else:
            extract_result = extract_number(value, False)

            # not a number
            if extract_result is None:
                return value, InputType("text")

            return extract_result

    elif current_type.name == "bool":
        if value.lower() in ["true", "t", "yes", "y", "1"]:
            return True, InputType("bool")

        elif value.lower() in ["false", "f", "no", "n", "0"]:
            return False, InputType("bool")

        else:
            return None, current_type

    elif current_type.name.startswith("timestamp"):
        if value == "" or value is None:
            return None, current_type

        # TODO: handle timezones
        result = re.findall(
            "\d+|jan|feb|mar|apr|jun|jul|aug|sep|oct|nov|dec", value.lower()
        )
        if len(result) < 3 or len(result) > 8:
            return value, InputType("text")

        dayfirst = None

        try:
            if current_type.params.get("dayfirst") is None:
                # FIXME: dayfirst = detect_dayfirst(current_type.params['last_value'])
                # dayfirst = detect_dayfirst(value)
                result, dayfirst = dateutil_parse(value)
            else:
                dayfirst = current_type.params["dayfirst"]
                result, dayfirst = dateutil_parse(value, dayfirst=dayfirst)

        except Exception as e:
            return value, InputType("text")

        return result, InputType("timestamp", dict(dayfirst=dayfirst, last_value=value))

    else:
        return value, current_type


def preprocess_csv_data(input_data):
    data = parse_csv(input_data)

    if len(data) < 2:
        raise ValueError("Not enough data")

    header = data[0]
    formated_header = format_header(header)

    return formated_header, header, data[1:]


#TODO is this even used?
def get_row_data_types(header, row, current_types=None, detect_type_func=None):
    if detect_type_func is None:
        detect_type_func = detect_type

    if current_types is None:
        current_types = {}

    new_types = dict(current_types)

    result_row = []

    for i, value in enumerate(row):
        if i >= len(header):
            break

        v, new_types[header[i]] = detect_type_func(value, current_types.get(header[i]))
        result_row.append(v)

    return result_row, new_types


def get_data_types(header, data, current_types=None, detect_type_func=None):
    if detect_type_func is None:
        detect_type_func = detect_type

    if current_types is None:
        current_types = {}

    result_data = []
    for row in data:
        result_row = []
        empty_count = 0

        for value in row:
            if value == "":
                empty_count += 1

        if empty_count == len(row):
            continue

        for i, value in enumerate(row):
            v, current_types[header[i]] = detect_type_func(
                value, current_types.get(header[i])
            )

            result_row.append(v)

        result_data.append(result_row)

    for h in header:
        if current_types.get(h) is None:
            current_types[h] = "text"

    return result_data, current_types


def format_header(header):
    formated_header = []
    collisions = {}
    generic_name_counter = 0

    for column in header:
        column = unidecode.unidecode(column)
        letters_and_digits = string.ascii_lowercase + string.digits
        buf = ""

        if len(column) > 0:
            for i, char in enumerate(column.lower()):
                if i == 0 and char in string.digits:
                    buf += "_"

                elif char not in letters_and_digits:
                    buf += "_"

                else:
                    buf += char

            # check for collisions
            if buf in formated_header:
                suffix = collisions.get(buf)

                if suffix is not None:
                    collisions[buf] = suffix + 1
                    buf = buf + "_" + str(suffix + 1)

                else:
                    buf += "_1"
        else:
            generic_name_counter += 1
            buf = "column_%i" % generic_name_counter

        formated_header.append(buf)

    return formated_header


if __name__ == "__main__":
    from icecream import ic

    ic(extract_number("100,001", False))
    ic(extract_number("100,001", True))
    ic(extract_number("100,001", None))

    ic(detect_type("50,000,001", InputType("int")))

    ic(extract_number("-25.40"))
    ic(detect_type(""))
