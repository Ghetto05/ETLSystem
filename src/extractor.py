import json
import os
import re
import pandas as pd
from pandas import isnull, DataFrame

# dynamically get path of script file and place log next to it
log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "log.txt")

def log(message, log_to_console):
    # optionally: print to console
    if log_to_console:
        print(message)
    # open log file, write output after new line, close files
    f = open(log_file_path, "a")
    f.write("\n")
    f.write(message)
    f.close()

def init_logger():
    # create log file
    file = open(log_file_path, "w")
    file.write("")
    file.close()

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
                            df.at[index, column] = pd.to_datetime(value, format="%Y.%m.%d")
                        elif expected_type == "datetime_DMY_.":
                            df.at[index, column] = pd.to_datetime(value, format="%d.%m.%Y")
                        elif expected_type == "datetime_MDY_.":
                            df.at[index, column] = pd.to_datetime(value, format="%m.%d.%Y")
                        elif expected_type == "datetime_YMD_-":
                            df.at[index, column] = pd.to_datetime(value, format="%Y-%m-%d")
                        elif expected_type == "datetime_DMY_-":
                            df.at[index, column] = pd.to_datetime(value, format="%d-%m-%Y")
                        elif expected_type == "datetime_MDY_-":
                            df.at[index, column] = pd.to_datetime(value, format="%m-%d-%Y")
                        else:
                            log(f"Unknown data type '{expected_type}' for column {column}.", True)
                        # apply value, in case it was changed below
                        df.at[index, column] = value
                        break
                    except ValueError:
                        # log event
                        log(f"Value '{value}' in column {column} at row {index} in file {file_name} does not match expected type {expected_type}",False)
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
                            log(f"Row {index} in file {file_name} removed by user.", False)
                            break
                        # ask user for new value
                        elif user_input == "e":
                            new_value = input(f"Enter new value for column '{column}': ")
                            value = new_value
                            log(f"Value '{new_value}' inserted into column {column} at row {index} in file {file_name} by user.", False)
                        else:
                            print("Invalid choice. Please enter 'r' to remove the row or 'e' to enter a new value.")
                            continue
    return df

def extract(folder, column_types) -> DataFrame:
    # gather references to all found files
    files = os.listdir(folder)
    csv_collection = [file for file in files if file.endswith('.csv')]
    xml_collection = [file for file in files if file.endswith('.xml')]
    json_collection = [file for file in files if file.endswith('.json')]

    # do not continue if no files found
    if not (csv_collection or xml_collection or json_collection):
        log("No CSV, XML, or JSON file in the specified folder.", False)
        raise ValueError("No CSV, XML, or JSON file in the specified folder.")

    data_frames = []
    for file in csv_collection:
        file_path = os.path.join(folder, file)
        try:
            log(f"Reading file {file}...", True)
            # read file
            df = pd.read_csv(file_path, sep=';')
            # validate data types
            df = validate_data_types(df, file, column_types)
            # append content to combined frame
            data_frames.append(df)
        except Exception as e:
            log(f"Could not read file {file}:\n{e}", True)

    for file in xml_collection:
        file_path = os.path.join(folder, file)
        try:
            log(f"Reading file {file}...", True)
            # read file
            df = pd.read_xml(file_path)
            # validate data types
            df = validate_data_types(df, file, column_types)
            # append content to combined frame
            data_frames.append(df)
        except Exception as e:
            log(f"Could not read file {file}: {e}", True)

    # load all json files
    for file in json_collection:
        file_path = os.path.join(folder, file)
        try:
            log(f"Reading file {file}...", True)
            # read file
            df = pd.read_json(file_path)
            # validate data types
            df = validate_data_types(df, file, column_types)
            # append content to combined frame
            data_frames.append(df)
        except Exception as e:
            log(f"Could not read file {file}: {e}", True)

    # check if data combination was successful
    if data_frames:
        combined_df = pd.concat(data_frames, ignore_index=True)
        log("Data combined successfully.", True)
        return combined_df
    else:
        log("No data could be combined.", True)
        raise ValueError("No data could be combined.")

#region transform helpers

def calc(value, calc_type):
    if pd.isnull(value):
        return None

    if calc_type == "AGE":
        try:
            # Parse the date string to a datetime object
            date_value = pd.to_datetime(value, format="%d-%m-%Y")
            today = pd.Timestamp.today()
            age = today.year - date_value.year - ((today.month, today.day) < (date_value.month, date_value.day))
            return age
        except Exception as e:
            log(f"Error calculating AGE for value '{value}': {e}", True)
            return None
    return None

def format_date(value, source_format, source_separator):
    if pd.isnull(value):
        return None

    try:
        if source_format == "DMY" and source_separator == "-":
            return pd.to_datetime(value, format="%d-%m-%Y").strftime("%d-%m-%Y")
        elif source_format == "DMY" and source_separator == ".":
            return pd.to_datetime(value, format="%d.%m.%Y").strftime("%d-%m-%Y")
        elif source_format == "MDY" and source_separator == "-":
            return pd.to_datetime(value, format="%m-%d-%Y").strftime("%d-%m-%Y")
        elif source_format == "MDY" and source_separator == ".":
            return pd.to_datetime(value, format="%m.%d.%Y").strftime("%d-%m-%Y")
        elif source_format == "YMD" and source_separator == "-":
            return pd.to_datetime(value, format="%Y-%m-%d").strftime("%d-%m-%Y")
        elif source_format == "YMD" and source_separator == ".":
            return pd.to_datetime(value, format="%Y.%m.%d").strftime("%d-%m-%Y")
        else:
            return value
    except Exception as e:
        log(f"Error formatting date '{value}' with source format '{source_format}' and separator '{source_separator}': {e}", False)
        return None

def preprocess_value(value, column_name, column_mapping, column_types):
    # if value is date, ensure uniform format is used
    if column_types[column_name].startswith("datetime_"):
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
        elif pd.isnull(value):
            return None
        else:
            return "X"

    # omit invalid mail addresses
    if target_column_types[target_column_name] == "mail":
        email_pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        if not re.match(email_pattern, value):
            return None

    return value

def transform_step(frame, value, column_name, row_index, column_mapping, colum_types, target_column_types):
    if pd.isnull(value):
        return

    log(f"Processing value '{value}' in column {column_name} at row {row_index}...", False)

    # preprocess value
    value = preprocess_value(value, column_name, column_mapping, colum_types)

    # discard value
    if column_mapping[column_name].startswith("§DISCARD"):
        return

    # calculate specified calculation type
    # example: '§CALC§AGE§member_age'
    elif column_mapping[column_name].startswith("§CALC"):
        target_column_name = column_mapping[column_name].split("§")[3]
        # get calculation type
        calc_type = column_mapping[column_name].split("§")[2]
        # calculate value
        calculated_value = calc(value, calc_type)
        calculated_value = postprocess_value(calculated_value, column_name, target_column_name, column_mapping, colum_types, target_column_types)
        # insert in mapped column
        frame.at[row_index, target_column_name] = calculated_value

    # split value and insert into specified columns
    # example: '§SPLIT§ §first_name§last_name' - Leon Becker > first_name=Leon, last_name=Becker
    elif column_mapping[column_name].startswith("§SPLIT"):
        # get separator char
        separator = column_mapping[column_name].split("§")[2]
        # get split value
        values = value.split(separator)
        # insert into mapped columns
        mapped_columns = column_mapping[column_name].split("§")[3:]
        for i, split_value in enumerate(values):
            if i < len(mapped_columns):
                target_column_name = mapped_columns[i]
                frame.at[row_index, target_column_name] = postprocess_value(split_value, column_name, target_column_name, column_mapping, colum_types, target_column_types)

    else:
        # insert value into mapped column
        target_column_name = column_mapping[column_name]
        frame.at[row_index, target_column_name] = postprocess_value(value, column_name, target_column_name, column_mapping, colum_types, target_column_types)

#endregion

def transform(frame, target_frame_columns, column_mapping, column_types) -> DataFrame:
    log("Transforming data into target format...", True)
    # Create a new DataFrame with columns in the specified order
    transformed_frame = pd.DataFrame(columns=target_frame_columns.keys())

    for index, row in frame.iterrows():
        for column, value in row.items():
            if column in column_mapping:  # Only process mapped columns
                transform_step(transformed_frame, value, column, index, column_mapping, column_types, target_frame_columns)

    log("Data transformed successfully.", True)
    return transformed_frame

def save_to_csv(frame, path):
    log(f"Saving results to {path}", True)
    frame.to_csv(path, index=False, sep=';')
    log("Results saved successfully.", True)

def main():
    init_logger()

    #region get folder and validate

    # prompt user for folder
    folder = input("Please enter path to files: ")
    log(f"User entered path: {folder}", False)

    # check if path exists
    if not os.path.exists(folder):
        log("The specified folder does not exist.", False)
        raise FileNotFoundError("The specified folder does not exist.")

    #endregion

    #region get format file

    # prompt user for format file
    format_file = input("Please enter path to format file: ")
    log(f"User entered format file path: {format_file}", False)

    # check if format file exists
    if not os.path.exists(format_file):
        log("The specified format file does not exist.", False)
        raise FileNotFoundError("The specified format file does not exist.")

    # load format from file
    with open(format_file, 'r', encoding='utf-8') as f:
        format_data = json.load(f)

    #endregion

    #region format and config values

    save_output_to_csv: bool = format_data['save_output_to_csv']
    output_csv_path: str = format_data['output_csv_path']
    column_types: dict[str, str] = format_data['column_types']
    column_mapping: dict[str, str] = format_data['column_mapping']
    target_frame_columns: dict[str, str] = format_data['target_frame_columns']

    # endregion

    # execute extract stage
    extracted_frame = extract(folder, column_types)

    # execute transform stage
    transformed_frame = transform(extracted_frame, target_frame_columns, column_mapping, column_types)
    # transformed_frame = extracted_frame

    # save data to csv
    if save_output_to_csv:
        save_to_csv(transformed_frame, output_csv_path)

if __name__ == '__main__':
    main()