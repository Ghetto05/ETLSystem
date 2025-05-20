An **Extract, Transform, Load (ETL)** solution designed to process structured data in a modular and customizable way.

## Project Structure

The project is organized into the following key components:
- **`src/main.py`**
  
  The entry point of the application. Handles initial input and gathering configuration data.

- **`src/extractor.py`**

  Contains functionality to extract data from various sources.

- **`src/transformer.py`**

  Processes and converts the extracted data into the desired format. It utilizes mapping rules described in the configuration.

- Format JSON accompanying the data
  
  A configuration file defining the:
    - source column names and data types
    - target column names, data types, required status, and column dependencies
    - transformation rules and mapping

## Configuration Overview

The transformation logic is defined in the in the format JSON file:
- **`column_types`**: Define the data types of source columns
- **`target_frame_columns`**: Specifies the desired target columns and their data types
- **`column_mapping`**: Maps source column names to target columns, and applies rules as specified

## Configuration - Rules and Mapping

The array `target_frame_columns` in the format JSON is used to map each source column to one target column.
Many source columns can be mapped to one target column, but each source column can only have one target.

TODO: explain rules
