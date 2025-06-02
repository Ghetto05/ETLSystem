import os

from pandas import DataFrame
from sqlalchemy import create_engine
from src import logger

# return size of the saved file
def save_to_csv(frame : DataFrame) -> int:
    logger.log("Saving results to CSV", False)
    path_valid = False
    path = ""
    while not path_valid:
        path = input("Enter target path (ending with '.csv'): ")
        if path.endswith(".csv"):
            path_valid = True
            logger.log(f"User entered path: {path}", False)
        else:
            print("Invalid path. Enter a valid path ending with '.csv'.")
            logger.log(f"Invalid path entered: {path}", False)
    logger.log(f"Saving results as CSV to {path}", True)
    frame.to_csv(path, index=False, sep=';')
    logger.log("Results saved successfully.", True)
    return os.path.getsize(path)


# return size of the saved file
def save_to_xml(frame : DataFrame) -> int:
    logger.log("Saving results to XML", False)
    path_valid = False
    path = ""
    while not path_valid:
        path = input("Enter target path (ending with '.xml'): ")
        if path.endswith(".xml"):
            path_valid = True
            logger.log(f"User entered path: {path}", False)
        else:
            print("Invalid path. Enter a valid path ending with '.xml'.")
            logger.log(f"Invalid path entered: {path}", False)
    logger.log(f"Saving results as XML to {path}", True)
    frame.to_xml(path, index=False)
    logger.log("Results saved successfully.", True)
    return os.path.getsize(path)


# return size of the saved file
def save_to_json(frame : DataFrame) -> int:
    logger.log("Saving results to JSON", False)
    path_valid = False
    path = ""
    while not path_valid:
        path = input("Enter target path (ending with '.json'): ")
        if path.endswith(".json"):
            path_valid = True
            logger.log(f"User entered path: {path}", False)
        else:
            print("Invalid path. Enter a valid path ending with '.json'.")
            logger.log(f"Invalid path entered: {path}", False)
    logger.log(f"Saving results as JSON to {path}", True)
    frame.to_json(path, index=False)
    logger.log("Results saved successfully.", True)
    return os.path.getsize(path)


# return size of the saved file
def save_to_sql(frame: DataFrame) -> int:
    logger.log("Saving results to SQL database", False)
    path_valid = False
    path = ""
    while not path_valid:
        path = input("Enter target path (ending with '.db'): ")
        if path.endswith(".db"):
            path_valid = True
            logger.log(f"User entered path: {path}", False)
        else:
            print("Invalid path. Enter a valid path ending with '.db'.")
            logger.log(f"Invalid path entered: {path}", False)

    table_name = input("Enter the table name to save data: ")
    engine = create_engine(f"sqlite:///{path}")
    print("Saving to database...")
    logger.log(f"Attempting to save to table '{table_name}' in database: {path}", False)

    frame.to_sql(name=table_name, con=engine, if_exists="replace", index=False)
    logger.log("Results saved successfully.", True)
    return os.path.getsize(path)


# return size of the saved file
def save(frame : DataFrame) -> int:
    valid_input = False
    file_size = 0
    while not valid_input:
        target = input("Enter target format [csv, json, xml, sql]: ")
        if target == "csv":
            valid_input = True
            logger.log(f"User chose target format CSV", False)
            file_size = save_to_csv(frame)
        elif target == "json":
            valid_input = True
            logger.log(f"User chose target format JSON", False)
            file_size = save_to_json(frame)
        elif target == "xml":
            valid_input = True
            logger.log(f"User chose target format XML", False)
            file_size = save_to_xml(frame)
        elif target == "sql":
            valid_input = True
            logger.log(f"User chose to save to database", False)
            file_size = save_to_sql(frame)
        else:
            print("Invalid input. Enter a valid target format.")
            logger.log(f"Invalid input entered: {target}", False)
    return file_size