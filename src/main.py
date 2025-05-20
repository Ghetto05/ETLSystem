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
    extracted_frame = extractor.extract(folder, column_types)

    # execute transform stage
    transformed_frame = transformer.transform(extracted_frame, target_frame_columns, column_mapping, column_types)

    # save data
    loader.save(transformed_frame)


if __name__ == '__main__':
    main()
