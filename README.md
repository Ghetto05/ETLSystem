## An ***Extract, Transform, Load (ETL)*** solution designed to process structured data in a modular and customizable way.

## Project Structure

The project is organized into the following key components:
- `src/main.py`
  
  The entry point of the application. Handles initial input and gathering configuration data.

- `src/extractor.py`

  Contains functionality to extract data from various sources.

- `src/transformer.py`

  Processes and converts the extracted data into the desired format. It utilizes mapping rules described in the configuration.

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

## Configuration - Rules and Mapping

The array `target_frame_columns` in the format JSON is used to map each source column to one target column.<br/>
Many source columns can be mapped to one target column, but each source column can only have one target.

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