from json import load
from os import path
from src import logger, extractor, transformer, loader


def get_data_folder():
    # prompt user for folder
    folder = input("Please enter path to files: ")
    logger.log(f"User entered path: {folder}", False)

    # check if path exists
    if not path.exists(folder):
        logger.log("The specified folder does not exist.", False)
        raise FileNotFoundError("The specified folder does not exist.")

    return folder


def get_format_file():
    # prompt user for format file
    format_file = input("Please enter path to format file: ")
    logger.log(f"User entered format file path: {format_file}", False)

    # check if format file exists
    if not path.exists(format_file):
        logger.log("The specified format file does not exist.", False)
        raise FileNotFoundError("The specified format file does not exist.")

    # load format from file
    with open(format_file, 'r', encoding='utf-8') as f:
        format_data = load(f)

    return format_data


def format_bytes(value) -> str:
    # return a string containing the bytes converted to the fitting unit and the unit symbol
    symbol = "B"
    return_value = value
    if value < 1024:
        symbol = "B"
        return_value = value
    elif value < 1024**2:
        symbol = "KiB"
        return_value = value / 1024
    elif value < 1024**3:
        symbol = "MiB"
        return_value = value / 1024**2
    elif value < 1024**4:
        symbol = "GiB"
        return_value = value / 1024**3
    elif value < 1024**5:
        symbol = "TiB"
        return_value = value / 1024**4
    return f"{return_value:.2f}".rstrip('00').rstrip('.') + symbol


def print_statistic(source_rows, source_bytes, target_rows, target_bytes):
    statistic = "Statistics:\n"
    statistic += f"Source data rows: {source_rows}\n"
    statistic += f"Source data bytes: {format_bytes(source_bytes)}\n"
    statistic += f"Target data rows: {target_rows}\n"
    statistic += f"Target data bytes: {format_bytes(target_bytes)}\n"
    statistic += f"Data row reduction: {(source_rows - target_rows) / source_rows * 100:.2f}%\n"
    statistic += f"Data size reduction: {(source_bytes - target_bytes) / source_bytes * 100:.2f}%"
    logger.log("\n\n" + statistic, True)
    save = input("\nSave statistics to disc? [y, n]:")
    if save.lower() == 'y':
        print("Saving statistics...")
        logger.log("Saving statistics to disc", False)
        with open(path.join(path.dirname(path.abspath(__file__)), "log.txt"), 'w') as f:
            f.write(statistic)
        print("Statistics saved")


def main():
    # create logger file or clear content if exists
    logger.init_logger()

    # get folder with data files
    folder = get_data_folder()

    # get file with data format information
    format_data = get_format_file()

    # format and config values
    column_types: dict[str, str] = format_data['column_types']
    column_mapping: dict[str, str] = format_data['column_mapping']
    target_frame_columns: dict[str, str] = format_data['target_frame_columns']

    # execute extract stage
    extracted_data = extractor.extract(folder, column_types)
    extracted_frame = extracted_data[0]
    source_data_rows = extracted_data[1]
    source_data_bytes = extracted_data[2]

    # execute transform stage
    transformed_frame = transformer.transform(extracted_frame, target_frame_columns, column_mapping, column_types)
    target_data_rows = len(transformed_frame)

    # save data
    target_data_bytes = loader.save(transformed_frame)

    # print statistic
    print_statistic(source_data_rows, source_data_bytes, target_data_rows, target_data_bytes)


if __name__ == '__main__':
    main()
