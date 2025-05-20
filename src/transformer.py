from re import match
from pandas import isnull, DataFrame, Timestamp, to_datetime
from src import logger


def calc(value, calc_type):
    if isnull(value):
        return None

    if calc_type == "AGE":
        try:
            # parse the date string to a datetime object
            date_value = to_datetime(value, format="dd-mm-YYYY")
            today = Timestamp.today()
            # calculate the age based on the date
            age = today.year - date_value.year - ((today.month, today.day) < (date_value.month, date_value.day))
            return age
        except Exception as e:
            logger.log(f"Error calculating AGE for value '{value}': {e}", True)
            return None
    elif calc_type == "BIRTHDATE":
        try:
            # calculate the date of birth assuming today is the birthday
            today = Timestamp.today()
            birthday_year = today.year - int(value)
            birth_date = Timestamp(year=birthday_year, month=today.month, day=today.day)
            return birth_date.strftime("%d-%m-%Y")
        except Exception as e:
            logger.log(f"Error calculating BIRTHDAY for age '{value}': {e}", True)
            return None

    return None


def format_date(value, source_format, source_separator):
    if isnull(value):
        return None

    try:
        if source_format == "DMY" and source_separator == "-":
            return to_datetime(value, format="%d-%m-%Y").strftime("%d-%m-%Y")
        elif source_format == "DMY" and source_separator == ".":
            return to_datetime(value, format="%d.%m.%Y").strftime("%d-%m-%Y")
        elif source_format == "MDY" and source_separator == "-":
            return to_datetime(value, format="%m-%d-%Y").strftime("%d-%m-%Y")
        elif source_format == "MDY" and source_separator == ".":
            return to_datetime(value, format="%m.%d.%Y").strftime("%d-%m-%Y")
        elif source_format == "YMD" and source_separator == "-":
            return to_datetime(value, format="%Y-%m-%d").strftime("%d-%m-%Y")
        elif source_format == "YMD" and source_separator == ".":
            return to_datetime(value, format="%Y.%m.%d").strftime("%d-%m-%Y")
        else:
            return value
    except Exception as e:
        logger.log(
            f"Error formatting date '{value}' with source format '{source_format}' and separator '{source_separator}': {e}",
            False)
        return None


def preprocess_value(value, column_name, column_mapping, column_types):
    # if value is date, ensure uniform format is used
    if column_types[column_name].startswith("datetime_"):
        logger.log(f"Formatting date '{value}' in column {column_name}...", False)
        return format_date(value, column_types[column_name].split("_")[1], column_types[column_name].split("_")[2])
    return value


def postprocess_value(value, column_name, target_column_name, column_mapping, column_types, target_column_types):
    # format bools as true = 1 and false = 0
    if target_column_types[target_column_name] == "bool":
        value = value.lower()
        if value == "true" or value == "1" or value == "yes" or value == "y" or value == "ja" or value == "j":
            return "1"
        else:
            return "0"

    # format gender as M, F or X
    if target_column_types[target_column_name] == "gender":
        value = value.lower()
        if value == "m" or value == "male" or value == "männlich" or value == "maennlich":
            return "M"
        elif value == "f" or value == "female" or value == "weiblich":
            return "F"
        elif isnull(value):
            return None
        else:
            return "X"

    # omit invalid mail addresses
    if target_column_types[target_column_name] == "mail":
        email_pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        if not match(email_pattern, value):
            return None

    return value


def transform_step(frame, value, column_name, row_index, column_mapping, colum_types, target_column_types):
    if isnull(value):
        return

    logger.log(f"Processing value '{value}' in column {column_name} at row {row_index}...", False)

    # preprocess value
    value = preprocess_value(value, column_name, column_mapping, colum_types)

    # discard value
    if column_mapping[column_name].startswith("§DISCARD"):
        logger.log(f"Discarding value '{value}' in column {column_name} at row {row_index}...", False)
        return

    # calculate specified calculation type
    # example: '§CALC§AGE§member_age'
    elif column_mapping[column_name].startswith("§CALC"):
        target_column_name = column_mapping[column_name].split("§")[3]
        # get calculation type
        calc_type = column_mapping[column_name].split("§")[2]
        # calculate value
        logger.log(f"Calculating {calc_type} for value '{value}' in column {column_name} at row {row_index}...", False)
        calculated_value = calc(value, calc_type)
        calculated_value = postprocess_value(calculated_value, column_name, target_column_name, column_mapping,
                                             colum_types, target_column_types)
        # insert in mapped column
        frame.at[row_index, target_column_name] = calculated_value

    # split value and insert into specified columns
    # example: '§SPLIT§ §first_name§last_name' - Leon Becker > first_name=Leon, last_name=Becker
    elif column_mapping[column_name].startswith("§SPLIT"):
        # get separator char
        separator = column_mapping[column_name].split("§")[2]
        # get split value
        logger.log(f"Splitting value '{value}' in column {column_name} at row {row_index} with separator '{separator}'...", False)
        values = value.split(separator)
        # insert into mapped columns
        mapped_columns = column_mapping[column_name].split("§")[3:]
        for i, split_value in enumerate(values):
            if i < len(mapped_columns):
                target_column_name = mapped_columns[i]
                frame.at[row_index, target_column_name] = postprocess_value(split_value, column_name,
                                                                            target_column_name, column_mapping,
                                                                            colum_types, target_column_types)

    else:
        # insert value into mapped column
        logger.log(f"Inserting value '{value}' in column {column_name} at row {row_index}...", False)
        target_column_name = column_mapping[column_name]
        frame.at[row_index, target_column_name] = postprocess_value(value, column_name, target_column_name,
                                                                    column_mapping, colum_types, target_column_types)


def transform(frame, target_frame_columns, column_mapping, column_types) -> DataFrame:
    logger.log("Transforming data into target format...", True)
    # create a new DataFrame with columns in the specified order
    transformed_frame = DataFrame(columns=target_frame_columns.keys())

    for index, row in frame.iterrows():
        for column, value in row.items():
            if column in column_mapping:  # Only process mapped columns
                transform_step(transformed_frame, value, column, index, column_mapping, column_types,
                               target_frame_columns)

    logger.log("Data transformed successfully.", True)
    return transformed_frame
