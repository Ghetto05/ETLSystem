## An *Extract, Transform, Load (ETL)* solution designed to process structured data in a modular and customizable way.

- [Usage](#usage)
- [Project Structure](#project-structure)
- [Configuration Overview](#configuration-overview)
- [Configuration - Tags and Mapping](#configuration---tags-and-mapping)
- [Configuration - Data Types](#configuration---data-types)
- [Logging](#logging)
- [Statistics](#statistics)

## Usage

To use this program, you need a folder containing table data in either CSV, JSON or XML files.
You also need a json file defining the format of the data and the mapping into the desired target table (see [below](#configuration-overview)).<br/>

Sample data for demonstration purposes has been supplied in `data/`, containing one CSV, JSON and XML file each.
The `format.json` file contains a pre-made mapping tailored to these files and can also be found in the `data/` folder.<br/>
To use this data, enter `data/` for the data folder and `data/format.json` for the format file.

To start the program, you first need to restore the virtual environment.
Refer to the file `how-to-venv.txt` for the command to restore the environment and execute it in the terminal.
The program can be started by running the `main.py` file.

Upon starting the program, you will be prompted to enter a path to your data files.
A relative path will be accepted, but a full path is preferable.<br/>
Afterward, you will be prompted for a format file. Again, relative paths are accepted.

During the extraction stage, any values that do not fit the expected datatype specified in the format file will be brought to attention.
You are then given the option to either remove the row by entering `r` or replace the value with `e`.
The prompt will reappear if the entered value does not fit the datatype as well.

Once the extraction and transformation steps are completed, you will be asked what format the data should be stored in.
After making your choice, you will be prompted for a save path.
Once entered, the data will be saved and the operation will be completed.

## Project Structure

The project is organized into the following key components:
- `src/main.py`
  
  The entry point of the application. Handles initial input and gathering configuration data.

- `src/extractor.py`

  Contains functionality to extract data from CSV, JSON and XML files.

- `src/transformer.py`

  Processes and converts the extracted data into the desired format. It utilizes mapping rules described in the format file.

- `src/loader.py`

  Saves the data in either CSV, JSON, XML or as a SQLite Database

- Format JSON accompanying the data
  
  A configuration file defining the:
    - source column names and data types
    - target column names, data types, required status, and column dependencies
    - transformation rules and mapping

## Configuration Overview

The transformation logic is defined in the format JSON file:
- `column_types`: Define the data types of source columns
- `target_frame_columns`: Specifies the desired target columns and their data types
- `column_mapping`: Maps source column names to target columns, and applies rules as specified

## Configuration - Tags and Mapping

The array `target_frame_columns` in the format JSON is used to map each source column to one target column.<br/>
Many source columns can be mapped to one target column, but each source column can only have one target.

There are some tags that can be used to process values before they are transformed into their target column:
- `§DISCARD`:

  Discard the value and skip the column
- `§CALC§[calculation type]§[target column]`:

  Calculate a value based on the cell<br/>The following calculation types exist:
  - `AGE`: Calculate how many years have passed
  - `BIRTHDATE`: Calculate a birthdate based on the current day

- `§SPLIT$[separator character]§[target column 1]§[target column 2]...`

  Splits the column's value using the specified separator character<br/>
Any number of target columns can be defined and the split results will be distributed in order<br/>
Any target columns exceeding the split result count will not receive a value and will remain empty

## Configuration - Data Types

These default data types can be used in either of the column definitions:
- `str`: String (text)
- `int`: Integer (non-decimal number)
- `float`: Float (decimal number)

To aid with validation and formatting, there are some custom data types that can be used in the `target_frame_columns`:
- `bool`: Converts values such as `Yes, y, j, Ja, 1` to `1` for `true` and `0` for `false`
- `gender`: Converts values such as `Männlich, M, male` to `M` for male and `F` for female, or `X` otherwise
- `mail`: Validates given E-Mail addresses and omits them if they are invalid

Further, there are date data types which can also be used in the `column_types`.<br/>
It is important to note that these types ***must*** be assigned correctly in accordance with the data source column format,
or extraction will fail.
- `datetime_DMY_.`: Dates in the `dd.mm.YYYY` format
- `datetime_MDY_.`: Dates in the `mm.dd.YYYY` format
- `datetime_YMD_.`: Dates in the `YYYY.mm.dd` format
- `datetime_DMY_-`: Dates in the `dd-mm-YYYY` format
- `datetime_MDY_-`: Dates in the `mm-dd-YYYY` format
- `datetime_YMD_-`: Dates in the `YYYY-mm-dd` format

Appending the tag `§REQUIRED` after a data type marks the column to be required.<br/>
Any row without a value for the column will be discarded before saving.

## Logging

In addition to the simplified output shown in the console, there is a text file containing verbose output.
It is called `log.txt` and will be placed next to the `main.py` file.

Any prompt, action and user input is logged here.
On any checks it will tell you which cells are being processed and which values are found.
On automatic actions it will state, for example, which row is being deleted or which value is being modified.

This serves to better understand the process and clear any confusion on why certain values exist
or to find issues with the format file or the data itself.

## Statistics

After the operation completed, you will be given a statistic of it.<br/>
It will show the following:
- `Source data rows`: The total count of rows found in the source data
- `Source data bytes`: The total size of all source data files
- `Target data rows`: The count of rows saved after completion
- `Target data bytes`: The size of the saved file
- `Data row reduction`: The difference between the row counts of the source and saved data
- `Data size reduction`: The difference in size between the source data files and the saved file

After the statistics are printed, you are given the option to save them to the disc.
Entering `y` will save them to `statistics.txt` (placed next to `main.py` and `log.txt`),
any other key will finish the operation without saving them.