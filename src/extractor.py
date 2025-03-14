import os
import pandas as pd

log_file_name = "log.txt"
output_file_name = "output.csv"

def log(message, log_to_console):
    if log_to_console:
        print(message)
    f = open(log_file_name, "a")
    f.write("\n")
    f.write(message)
    f.close()

def init_logger():
    file = open(log_file_name, "w")
    file.write("")
    file.close()

def main():
    init_logger()

    folder = input("Please enter path to files: ")
    log(f"User entered path: {folder}", False)

    if not os.path.exists(folder):
        log("The specified folder does not exist.", False)
        raise FileNotFoundError("The specified folder does not exist.")

    files = os.listdir(folder)

    csvs = [file for file in files if file.endswith('.csv')]
    xmls = [file for file in files if file.endswith('.xml')]
    jsons = [file for file in files if file.endswith('.json')]

    if not (csvs or xmls or jsons):
        log("No CSV, XML, or JSON file in the specified folder.", False)
        raise ValueError("No CSV, XML, or JSON file in the specified folder.")

    data_frames = []
    for file in csvs:
        file_path = os.path.join(folder, file)
        try:
            log(f"Reading file {file}...", True)
            df = pd.read_csv(file_path, sep=';')
            data_frames.append(df)
            df.to_csv("result_csv.csv", sep=';')
        except Exception as e:
            log(f"Could not read file {file}:\n{e}", True)

    for file in xmls:
        file_path = os.path.join(folder, file)
        try:
            log(f"Reading file {file}...", True)
            df = pd.read_xml(file_path)
            data_frames.append(df)
            df.to_csv("result_xml.csv", sep=';')
        except Exception as e:
            log(f"Could not read file {file}: {e}", True)

    for file in jsons:
        file_path = os.path.join(folder, file)
        try:
            log(f"Reading file {file}...", True)
            df = pd.read_json(file_path)
            data_frames.append(df)
            df.to_csv("result_json.csv", sep=';')
        except Exception as e:
            log(f"Could not read file {file}: {e}", True)

    if data_frames:
        combined_df = pd.concat(data_frames, ignore_index=True)
        log("Data combined successfully.", True)
    else:
        raise ValueError("No data could be combined.")

    print(combined_df)

    log(f"Saving results to {output_file_name}", True)
    combined_df.to_csv(output_file_name, index=False, sep=';')

main()