import os
import pandas as pd
from pandas import isnull

log_file_name = "log.txt"
output_file_name = "output.csv"

column_types = {
    "id": "int",
    "nr": "int",
    "mitglnr": "int",
    "vorname": "str",
    "nachname": "str",
    "name": "str",
    "name_kopie": "str",
    "aktiv": "str",
    "aktivesMitglied": "str",
    "geschlecht": "str",
    "geburstdatum": "datetime_DMY_.",
    "geburtstag": "datetime_YMD_-",
    "tel": "str",
    "email": "str",
    "alter": "int",
    "strasse": "str",
    "hausnr": "int",
    "haus": "int",
    "stadt": "str",
    "plz": "int",
    "adresse1": "str",
    "adresse2": "int",
    "adresse3": "str",
    "adresse4": "int",
    "verein1": "int",
    "verein2": "int",
    "verein3": "int",
    "mitgliedVereinA": "int",
    "mitgliedVereinB": "int",
    "vereinsnr": "int",
    "position": "str",
}

def validate_data_types(df, file_name):
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
                        user_input = input(
                            f"Error in file '{file_name}', column '{column}', row {index} with value '{value}'.\n"
                            f"Data type: {expected_type}\n"
                            "Would you like to [r]emove the row or [e]nter a new value?: "
                        ).strip().lower()
                        # remove row from table
                        if user_input == "r":
                            df = df.drop(index).reset_index(drop=True)
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

def log(message, log_to_console):
    # optionally: print to console
    if log_to_console:
        print(message)
    # open log file, write output after new line, close files
    f = open(log_file_name, "a")
    f.write("\n")
    f.write(message)
    f.close()

def init_logger():
    # create log file
    file = open(log_file_name, "w")
    file.write("")
    file.close()

def main():
    init_logger()

    # prompt user for folder
    folder = input("Please enter path to files: ")
    log(f"User entered path: {folder}", False)

    # check if path exists
    if not os.path.exists(folder):
        log("The specified folder does not exist.", False)
        raise FileNotFoundError("The specified folder does not exist.")

    # gather references to all found files
    files = os.listdir(folder)
    csvs = [file for file in files if file.endswith('.csv')]
    xmls = [file for file in files if file.endswith('.xml')]
    jsons = [file for file in files if file.endswith('.json')]

    # do not continue if no files found
    if not (csvs or xmls or jsons):
        log("No CSV, XML, or JSON file in the specified folder.", False)
        raise ValueError("No CSV, XML, or JSON file in the specified folder.")

    data_frames = []
    for file in csvs:
        file_path = os.path.join(folder, file)
        try:
            log(f"Reading file {file}...", True)
            # read file
            df = pd.read_csv(file_path, sep=';')
            # validate data types
            df = validate_data_types(df, file)
            # append content to combined frame
            data_frames.append(df)
        except Exception as e:
            log(f"Could not read file {file}:\n{e}", True)

    for file in xmls:
        file_path = os.path.join(folder, file)
        try:
            log(f"Reading file {file}...", True)
            # read file
            df = pd.read_xml(file_path)
            # validate data types
            df = validate_data_types(df, file)
            # append content to combined frame
            data_frames.append(df)
        except Exception as e:
            log(f"Could not read file {file}: {e}", True)

    # load all json files
    for file in jsons:
        file_path = os.path.join(folder, file)
        try:
            log(f"Reading file {file}...", True)
            # read file
            df = pd.read_json(file_path)
            # validate data types
            df = validate_data_types(df, file)
            # append content to combined frame
            data_frames.append(df)
        except Exception as e:
            log(f"Could not read file {file}: {e}", True)

    # check if data combination was successful
    if data_frames:
        combined_df = pd.concat(data_frames, ignore_index=True)
        log("Data combined successfully.", True)
    else:
        log("No data could be combined.", True)
        return;

    log(f"Saving results to {output_file_name}", True)
    combined_df.to_csv(output_file_name, index=False, sep=';')
    log("Results saved successfully.", True)

main()