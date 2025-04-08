import os
import pandas as pd

log_file_name = "log.txt"
output_file_name = "output.csv"

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
            # append content to combined frame
            data_frames.append(df)
        except Exception as e:
            log(f"Could not read file {file}: {e}", True)

    # check if data combination was successful
    if data_frames:
        combined_df = pd.concat(data_frames, ignore_index=True)
        log("Data combined successfully.", True)
    else:
        raise ValueError("No data could be combined.")

    log(f"Saving results to {output_file_name}", True)
    combined_df.to_csv(output_file_name, index=False, sep=';')

main()