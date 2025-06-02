from os import path, listdir
from pandas import isnull, DataFrame, to_datetime, read_csv, read_xml, read_json, concat
from src import logger


def validate_data_types(df: DataFrame, file_name, column_types):
    for column, expected_type in column_types.items():
        if column in df.columns:
            for index, value in df[column].items():
                # skip null values
                if isnull(value):
                    continue

                # try converting each cell to value, on error > prompt user
                while True:
                    try:
                        if expected_type == "int":
                            df.at[index, column] = int(value)
                        elif expected_type == "float":
                            df.at[index, column] = float(value)
                        elif expected_type == "str":
                            df.at[index, column] = str(value)
                        elif expected_type == "bool":
                            df.at[index, column] = bool(value)
                        elif expected_type == "datetime_YMD_.":
                            df.at[index, column] = to_datetime(value, format="%Y.%m.%d")
                        elif expected_type == "datetime_DMY_.":
                            df.at[index, column] = to_datetime(value, format="%d.%m.%Y")
                        elif expected_type == "datetime_MDY_.":
                            df.at[index, column] = to_datetime(value, format="%m.%d.%Y")
                        elif expected_type == "datetime_YMD_-":
                            df.at[index, column] = to_datetime(value, format="%Y-%m-%d")
                        elif expected_type == "datetime_DMY_-":
                            df.at[index, column] = to_datetime(value, format="%d-%m-%Y")
                        elif expected_type == "datetime_MDY_-":
                            df.at[index, column] = to_datetime(value, format="%m-%d-%Y")
                        else:
                            logger.log(f"Unknown data type '{expected_type}' for column {column}.", True)
                        # apply value, in case it was changed below
                        df.at[index, column] = value
                        break
                    except ValueError:
                        # log event
                        logger.log(
                            f"Value '{value}' in column {column} at row {index} in file {file_name} does not match expected type {expected_type}",
                            False)
                        # prompt user for action
                        # noinspection SpellCheckingInspection
                        user_input = input(
                            f"Error in file '{file_name}', column '{column}', row {index} with value '{value}'.\n"
                            f"Data type: {expected_type}\n"
                            "Would you like to [r]emove the row or [e]nter a new value?: "
                        ).strip().lower()
                        # remove row from table
                        if user_input == "r":
                            df = df.drop(index)
                            logger.log(f"Row {index} in file {file_name} removed by user.", False)
                            break
                        # ask user for new value
                        elif user_input == "e":
                            new_value = input(f"Enter new value for column '{column}': ")
                            value = new_value
                            logger.log(
                                f"Value '{new_value}' inserted into column {column} at row {index} in file {file_name} by user.",
                                False)
                        else:
                            print("Invalid choice. Please enter 'r' to remove the row or 'e' to enter a new value.")
                            continue
    return df


# return combined data frame, total amount of columns in source data and combined size of source data
def extract(folder, column_types) -> tuple[DataFrame, int, int]:
    total_source_rows = 0
    total_source_bytes = 0

    # gather references to all found files
    files = listdir(folder)
    csv_collection = [file for file in files if file.endswith('.csv')]
    xml_collection = [file for file in files if file.endswith('.xml')]
    json_collection = [file for file in files if file.endswith('.json')]

    # do not continue if no files found
    if not (csv_collection or xml_collection or json_collection):
        logger.log("No CSV, XML, or JSON file in the specified folder.", False)
        raise ValueError("No CSV, XML, or JSON file in the specified folder.")

    data_frames = []
    for file in csv_collection:
        file_path = path.join(folder, file)
        try:
            logger.log(f"Reading file {file}...", True)
            # read file
            df = read_csv(file_path, sep=';')
            # save statistics
            total_source_rows += len(df)
            total_source_bytes += path.getsize(file_path)
            # validate data types
            df = validate_data_types(df, file, column_types)
            # append content to combined frame
            data_frames.append(df)
        except Exception as e:
            logger.log(f"Could not read file {file}:\n{e}", True)

    for file in xml_collection:
        file_path = path.join(folder, file)
        try:
            logger.log(f"Reading file {file}...", True)
            # read file
            df = read_xml(file_path)
            # save statistics
            total_source_rows += len(df)
            total_source_bytes += path.getsize(file_path)
            # validate data types
            df = validate_data_types(df, file, column_types)
            # append content to combined frame
            data_frames.append(df)
        except Exception as e:
            logger.log(f"Could not read file {file}: {e}", True)

    # load all json files
    for file in json_collection:
        file_path = path.join(folder, file)
        try:
            logger.log(f"Reading file {file}...", True)
            # read file
            df = read_json(file_path)
            # save statistics
            total_source_rows += len(df)
            total_source_bytes += path.getsize(file_path)
            # validate data types
            df = validate_data_types(df, file, column_types)
            # append content to combined frame
            data_frames.append(df)
        except Exception as e:
            logger.log(f"Could not read file {file}: {e}", True)

    # check if data combination was successful
    if data_frames:
        frame = concat(data_frames, ignore_index=True)
        logger.log("Data combined successfully. Dropping duplicates...", True)
        # drop duplicated
        frame.drop_duplicates()
        return frame, total_source_rows, total_source_bytes
    else:
        logger.log("No data could be combined.", True)
        raise ValueError("No data could be combined.")
