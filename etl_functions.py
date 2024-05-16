import pandas as pd
import os
import time
import psycopg2
import psycopg2.extras as extras
import chardet
import re
import time
from tqdm import tqdm
import getpass

# import functions file
import sys
file = 'functions.py'
sys.path.insert(0,os.path.dirname(os.path.abspath(file)))
import functions


def compare_column_names(config_et_file_name, item_FILES_TO_PROCESS):
    """
    This function iterates over the provided files and compares their column names. It returns a tuple containing 
    two lists: the columns from the first file and the columns from other data frames that are not in the first one.

    Args:
        config_et_file_name (str): The path to the YAML configuration file.
        item_FILES_TO_PROCESS (str): The item identifier to retrieve from the YAML configuration.

    Returns:
        tuple: A tuple containing two lists - the list of columns from the first file and the list of 
               columns from other data frames that are not in the first one.
    """

    # run the function that detects errors in the YAML file structure
    yaml_errors = functions.detect_yaml_errors(config_et_file_name)

    if yaml_errors is not None:
        # Handle the error in phrasing YML file
        print(yaml_errors)

    else:

        # Get the path where the files to concatenate are stored using the get_path_item() function
        dir_path = functions.get_path_item(config_et_file_name, item_FILES_TO_PROCESS)
        
        files = [f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))]
        print('Files to compare: ')
        print(files)  # Print filenames only

        number = 0

        # If there is only 1 file it only will show colum names of that file
        if len(files) == 1:

            print('\nThe comparison cannot be made because there is only one file!')

        else:
        
            # Create a tqdm progress bar for the loop
            with tqdm(total=len(files), desc="Processing files") as pbar:
        
                # read each file
                for file in os.listdir(dir_path):
        
                    # if current path is a file do the column checking
                    if os.path.isfile(os.path.join(dir_path, file)):
        
                        path = dir_path + file
        
                        # detect encoding
        
                        with open(path, 'rb') as f:
                            
                            result = chardet.detect(f.read())
                        
                        encoding = result['encoding']
        
                        # / detect encoding
        
                        # read the first file
                        if number == 0:
                            dataFrame = pd.read_csv(path, encoding=encoding)
                            
                            # remove unnamed columns 
                            dataFrame = dataFrame.dropna(how='all', axis='columns')
        
                        # read the second file and compare columns with the columns of first dataframe
                        if number == 1:
        
                            newDataFrame = pd.read_csv(path, encoding=encoding)
                            
                            # remove unnamed columns 
                            newDataFrame = newDataFrame.dropna(how='all', axis='columns')
        
                            # create a list with the columns names that are different
                            lst_difference = newDataFrame.columns.str.lower().difference(dataFrame.columns.str.lower()).values.tolist()
        
                        else:
        
                            newDataFrame = pd.read_csv(path, encoding=encoding)
                            
                            # remove unnamed columns 
                            newDataFrame = newDataFrame.dropna(how='all', axis='columns')
        
                            # compare column names that are different
                            difference = newDataFrame.columns.str.lower().difference(dataFrame.columns.str.lower()).values.tolist()
        
                            # add the column name to the ist if they does not exist
                            for element in difference:
                                lst_difference.append(element) if element not in lst_difference else lst_difference
        
                        number += 1
                        pbar.update(1)  # Update progress bar
        
                print('\nOutput explanation: \n \n ([List columns on first file], \n [List columns NOT on first file])\n')
        
                return dataFrame.columns.values.tolist(), lst_difference


def extract_and_join_files(config_et_file_name, item_FILES_TO_PROCESS, item_UPDATE_COLUMN_NAMES, item_ADDITIONAL_INFORMATION_FILES,
                       item_JOIN_FILES_COMMON_COLUMNS):
    """
    This function extracts and joins files located in the specified directory. If additional information files 
    are provided, their columns are added to the main DataFrame based on the common columns specified in the 
    configuration under the JOIN_FILES_COMMON_COLUMNS block.

    Args:
        config_et_file_name (str): The path to the YAML configuration file.
        item_FILES_TO_PROCESS (str): The item identifier to retrieve files to process from the YAML configuration.
        item_UPDATE_COLUMN_NAMES (str): The item identifier to retrieve column name updates from the YAML configuration.
        item_ADDITIONAL_INFORMATION_FILES (str): The item identifier to retrieve additional information files from the YAML configuration.
        item_JOIN_FILES_COMMON_COLUMNS (str): The item identifier to retrieve common columns for joining files from the YAML configuration.

    Returns:
        pandas.DataFrame: The joined DataFrame containing data from the processed files and additional information files, if any.
    """

    # run the function that detects errors in the YAML file structure
    yaml_errors = functions.detect_yaml_errors(config_et_file_name)

    if yaml_errors is not None:
        # Handle the error in phrasing YML file
        print(yaml_errors)

        print('\nNO DATA FRAME WAS CREATED!')

    else:

        # get and concatenate all the files that are going to be proccessed. Function concatenate_csv_files() is used
        data_frame_to_process = functions.concatenate_csv_files_updating_column_names(config_et_file_name, item_FILES_TO_PROCESS, item_UPDATE_COLUMN_NAMES)
        
        # remove unnamed columns 
        data_frame_to_process = data_frame_to_process.dropna(how='all', axis='columns')

        ### If there is additional information to add, join to the dataframe. If not only change the name of the data frame
        # ADDITIONAL_INFORMATION_FILES
        # JOIN_FILES_COMMON_COLUMNS
        additional_information_files_path = functions.get_yml_item_value(config_et_file_name, item_ADDITIONAL_INFORMATION_FILES)

        #print(f'additional_information_files_path: {additional_information_files_path}')

        if additional_information_files_path != None:
            
            # get and concatenate all the additional info files that are going to be proccessed. Function concatenate_csv_files() is used
            additional_information_files = functions.concatenate_csv_files(config_et_file_name, item_ADDITIONAL_INFORMATION_FILES)
        
            # remove unnamed columns 
            additional_information_files = additional_information_files.dropna(how='all', axis='columns')

        else:

            print("No values provided for the ADDITIONAL_INFORMATION_FILESblock in the configuration file.")
        
        # if directory path was specified in the configution file and the folder contains the additional files, they are going to be 
        # joined to the data files
        if additional_information_files_path != None and additional_information_files is not None:

            # create a list whit the name of the dataFames
            dataFrame_list = [additional_information_files, data_frame_to_process]

            # get the list of columns that are going to be used to join the dataframes
            columns_join = functions.get_yml_item_value(config_et_file_name, item_JOIN_FILES_COMMON_COLUMNS)

            columns_join_lower = [re.sub(r'\s+', '_', s.strip().lower()) for s in columns_join]

            for dataFrame in dataFrame_list:

                # check if the columns to be used to join the dataframes that are writen in the config file exist in dataframe
                set_of_columns = functions.check_if_list_columns_exist_in_dataframe(columns_join_lower, dataFrame)

                if isinstance(set_of_columns, set):

                    print(f"The following columns do not exist in the data frame: {', '.join(set(columns_join_lower).difference(dataFrame.columns))}\nPlease fix it!")

                    print('\nColumns in data files:',list(data_frame_to_process.columns))

                    print('\nColumns in iditional information file:',list(additional_information_files.columns))

                    break 

            joined_dataFrame = pd.merge(data_frame_to_process, additional_information_files, on = columns_join_lower, how = 'left') 

            print('\nNew dataframe info:\n')

            print(joined_dataFrame.info())
            
            return joined_dataFrame

        else:

            print('\nNo additional information was added to the joined files.')

            print('\nNew dataframe info:\n')
                
            print(data_frame_to_process.info())

            return data_frame_to_process


def drop_not_used_columns(config_et_file_name, item_COLUMNS_TO_DROP, dataFrame):
    """
    This function drops unused columns from the DataFrame based on the configuration provided.

    Args:
        config_et_file_name (str): The path to the YAML configuration file.
        item_COLUMNS_TO_DROP (str): The item identifier to retrieve columns to drop from the YAML configuration.
        dataFrame (pandas.DataFrame): The DataFrame from which columns are to be dropped.

    Returns:
        pandas.DataFrame: The DataFrame with unused columns dropped.
    """ 

    # Check if DataFrame is None
    if dataFrame is None:
        print("Error: The data frame provided is a Null object.\nPlease review if the previous function was well executed.")
        return

    # run the function that detects errors in the YAML file structure
    yaml_errors = functions.detect_yaml_errors(config_et_file_name)

    if yaml_errors is not None:
        # Handle the error in phrasing YML file
        print(yaml_errors)

        print('\nNO DATA FRAME WAS CREATED!')

    else:

        # get the list of columns that are going to be used to join the dataframes
        columns_to_drop = functions.get_yml_item_value(config_et_file_name, item_COLUMNS_TO_DROP)

        # if there is any value in the config file to drop, run the following code to drop the listed columns
        if not isinstance(columns_to_drop, type(None)):

            columns_to_drop_lower = [re.sub(r'\s+', '_', s.strip().lower()) for s in columns_to_drop]
            print('Columns to drop:', columns_to_drop_lower)
            
            # Initialize tqdm progress bar
            with tqdm(total=len(columns_to_drop_lower), desc="Dropping columns") as pbar:

                # drop columns from the dataframe
                for column_list in columns_to_drop_lower:

                    # delete if column of dataframe exists in list of columns
                    if column_list in dataFrame.columns:

                        #del dataFrame[col_df]
                        dataFrame = dataFrame.drop(columns=column_list)

                    pbar.update(1)  # Update progress bar

            print('\nNew dataframe info:\n')

            print(dataFrame.info())

            return dataFrame

        else:

            print("No values provided for the COLUMNS_TO_DROP block in the configuration file. No updates performed.")

            # print('\nDataframe info: \n')
                
            # print(dataFrame.head())

            return dataFrame


def add_new_columns(config_et_file_name, item_NEW_COLUMNS, dataFrame): 
    """
    This function adds new columns to the DataFrame based on the configuration provided.

    Args:
        config_et_file_name (str): The path to the YAML configuration file.
        item_NEW_COLUMNS (str): The item identifier to retrieve new columns from the YAML configuration.
        dataFrame (pandas.DataFrame): The DataFrame to which new columns are to be added.

    Returns:
        pandas.DataFrame: The DataFrame with new columns added.
    """

    # Check if DataFrame is None
    if dataFrame is None:
        print("Error: The data frame provided is a Null object.\nPlease review if the previous function was well executed.")
        return

    # run the function that detects errors in the YAML file structure
    yaml_errors = functions.detect_yaml_errors(config_et_file_name)

    if yaml_errors is not None:
        # Handle the error in phrasing YML file
        print(yaml_errors)

        print('\nNO DATA FRAME WAS CREATED!')

    else:

        # new columns to add 
        new_columns = functions.get_yml_item_value(config_et_file_name, item_NEW_COLUMNS)

        if new_columns != None:

            total_columns = len(new_columns)  # Total number of new columns

            # Initialize tqdm progress bar
            with tqdm(total=total_columns, desc="Adding new columns") as pbar:

                number = len(dataFrame.columns)  # it will be used to get the dataframe position of new columns

                for key_column in new_columns:

                    key_column_new = re.sub(r'\s+', '_', key_column.strip().lower())

                    # Check if the column already exists
                    if key_column_new not in dataFrame.columns:

                        dataFrame.insert(number, key_column_new, new_columns.get(key_column))

                        print(f"The following column was inserted: '{key_column_new}' : '{new_columns.get(key_column)}'")

                    else: 

                        print(f"Column '{key_column_new}' already exists.")

                    # Update progress bar
                    pbar.update(1)

                print('\nNew dataframe info:\n')
                    
                print(dataFrame.info())
                    
                return dataFrame

        else:

            print("No values provided for the NEW_COLUMNS block in the configuration file. No updates performed.")
            
            return dataFrame

def update_column_names(config_et_file_name, item_UPDATE_COLUMN_NAMES, dataFrame):
    """
    This function updates column names in the DataFrame based on the configuration provided.

    Args:
        config_et_file_name (str): The path to the YAML configuration file.
        item_UPDATE_COLUMN_NAMES (str): The key to retrieve the column name updates from the YAML configuration.
        dataFrame (pandas.DataFrame): The DataFrame whose column names are to be updated.

    Returns:
        pandas.DataFrame: The DataFrame with updated column names.
    """ 

    # Check if DataFrame is None
    if dataFrame is None:
        print("Error: The data frame provided is a Null object.\nPlease review if the previous function was well executed.")
        return

    # run the function that detects errors in the YAML file structure
    yaml_errors = functions.detect_yaml_errors(config_et_file_name)

    if yaml_errors is not None:
        # Handle the error in phrasing YML file
        print(yaml_errors)

        print('\nNO DATA FRAME WAS CREATED!')

    else:
        # run the function that detects errors in the YAML file structure
        yaml_errors = functions.detect_yaml_errors(config_et_file_name)

        if yaml_errors is not None:
            # Handle the error in phrasing YML file
            print(yaml_errors)

            print('\nNO DATA FRAME WAS CREATED!')

        else:

            # run the function that detects errors in the YAML file structure
            yaml_errors = functions.detect_yaml_errors(config_et_file_name)

            if yaml_errors is not None:
                # Handle the error in phrasing YML file
                print(yaml_errors)

                print('\nNO DATA FRAME WAS CREATED!')

            else:
            
                # run the function that detects errors in the YAML file structure
                yaml_errors = functions.detect_yaml_errors(config_et_file_name)

                if yaml_errors is not None:
                    # Handle the error in phrasing YML file
                    print(yaml_errors)

                    print('\nNO DATA FRAME WAS CREATED!')

                else:

                    # new columns to add 
                    update_column_names = functions.get_yml_item_value(config_et_file_name, item_UPDATE_COLUMN_NAMES)

                    # validate if there keys and values to update column names
                    if update_column_names is not None:

                        # Count total number of columns to update
                        total_columns = len(update_column_names)
                        
                        # Initialize tqdm progress bar
                        with tqdm(total=total_columns, desc="Updating column names") as pbar:

                            # update_column_names in the data frame
                            for key, value in update_column_names.items():

                                # Transform key and value to lowercase and replace spaces with underscores
                                new_value = re.sub(r'\s+', '_', value.strip().lower())
                                new_key = re.sub(r'\s+', '_', key.strip().lower())

                                # transform column names to lowercase and no spaces
                                dataFrame.columns = map(lambda x: re.sub(r'\s+', '_', x.strip().lower()), dataFrame.columns)

                                # Check if key exists in DataFrame columns
                                if new_key in dataFrame.columns:

                                    # Rename the column if it exists
                                    dataFrame.rename({new_key: new_value}, axis=1, inplace=True)

                                    print(f"'{key}' replaced by '{new_value}'.")

                                else:
                                    # Print a message if the key is not found in DataFrame columns
                                    print(f"Column '{key}' NOT FOUND in the DataFrame.")

                                # Update progress bar
                                pbar.update(1)

                        # create a new DataFrame that merges columns with same names together
                        merged_dataFrame = dataFrame.T.groupby(level=0).first().T

                        print('\nNew dataframe info:\n')

                        print(merged_dataFrame.info())
                                
                        return merged_dataFrame

                    else:

                        print("No values provided for the UPDATE_COLUMN_NAMES block in the configuration file. No updates performed.")

                        return dataFrame


def update_row_values(config_et_file_name, item_UPDATE_ROW_VALUES, dataFrame):
    """
    This function iterates over each column in the DataFrame and replaces specified values with new values 
    defined in the configuration file. Progress is tracked using a progress bar.

    Args:
        config_et_file_name (str): The path to the YAML configuration file.
        item_UPDATE_ROW_VALUES (str): The identifier for the block containing column value updates in the configuration file.
        dataFrame (pandas.DataFrame): The DataFrame whose column values are to be updated.

    Returns:
        pandas.DataFrame: The DataFrame with updated column values.
    """

    # Check if DataFrame is None
    if dataFrame is None:
        print("Error: The data frame provided is a Null object.\nPlease review if the previous function was well executed.")
        return

    # run the function that detects errors in the YAML file structure
    yaml_errors = functions.detect_yaml_errors(config_et_file_name)

    if yaml_errors is not None:
        # Handle the error in phrasing YML file
        print(yaml_errors)

        print('\nNO DATA FRAME WAS CREATED!')

    else:

        # get dict values from config file
        values_update =  functions.get_yml_item_value(config_et_file_name, item_UPDATE_ROW_VALUES)

        if values_update != None:

            # Initialize tqdm progress bar with the total number of columns to update
            with tqdm(total=len(values_update), desc="Updating column values") as pbar:

                # get values and keys from each dictionary element
                for key in values_update:

                    value = values_update[key]

                    # review each column to update the values    
                    for column in dataFrame.columns:

                        dataFrame[column].mask(dataFrame[column] == key, value, inplace=True)


                    print(f"The value '{key}' was replaced by '{value}'")

                    # Update progress bar
                    pbar.update(1)

                print('\nNew dataframe head: \n')
                        
                print(dataFrame.head())
                
                return dataFrame

        else:

            print("No values provided for the VALUES_TRANSFORM block in the configuration file. No updates performed.")

            return dataFrame


def transpose_multiple_columns_to_a_single_column(config_et_file_name, additional_config_file, item_TRANSPOSE_COLUMNS_TO_ONE_COLUMN, 
    item_NAMES_NEW_UNIQUE_COLUMNS, item_INVALID_VALUES, dataFrame):
    """
    This function transposes values from specified columns in the DataFrame to a single column, while also creating 
    additional columns for specifying measurement names and units. It retrieves the columns to transpose and the names 
    for the new unique columns from the provided YAML configuration file.

    Args:
        config_et_file_name (str): Path to the YAML configuration file.
        item_TRANSPOSE_COLUMNS_TO_ONE_COLUMN (str): Item identifier to retrieve columns to transpose from the YAML configuration.
        item_NAMES_NEW_UNIQUE_COLUMNS (str): Item identifier to retrieve names for new unique columns from the YAML configuration.
        dataFrame (pandas.DataFrame): DataFrame containing the data to transform.
        
    Returns:
        pandas.DataFrame: DataFrame with transposed columns and new unique columns added.
    """

    # Check if DataFrame is None
    if dataFrame is None:
        print("Error: The data frame provided is a Null object.\nPlease review if the previous function was well executed.")
        return

    # run the function that detects errors in the YAML file structure
    yaml_errors = functions.detect_yaml_errors(config_et_file_name)

    if yaml_errors is not None:
        # Handle the error in phrasing YML file
        print(yaml_errors)

        print('\nNO DATA FRAME WAS CREATED!')

    else:

        # get the list of colums to transpose into a single column
        unique_column_diferent_value = functions.get_yml_item_value(config_et_file_name, item_TRANSPOSE_COLUMNS_TO_ONE_COLUMN)
        
        if unique_column_diferent_value != None:
        
            # get new column label  values from NAMES_NEW_UNIQUE_COLUMNS
            new_unique_columns =  functions.get_yml_item_value(config_et_file_name, item_NAMES_NEW_UNIQUE_COLUMNS)

            
            if new_unique_columns != None:
        
                # Validate that values 
                if len(new_unique_columns) == 3:
                    
                    # asign new data frame column names 
                    variable_value_label = re.sub(r'\s+', '_', new_unique_columns[0].strip().lower())
                    variable_units_label = re.sub(r'\s+', '_', new_unique_columns[1].strip().lower())
                    variable_name_label = re.sub(r'\s+', '_', new_unique_columns[2].strip().lower())
            
                    # Make a list of columns from the configuration file that are not in the data frame. 
                    columns_in_df = [col for col in unique_column_diferent_value if re.sub(r'\s+', '_', col.strip().lower()) in dataFrame.columns]
                    
                    columns_not_in_df = [col for col in unique_column_diferent_value if re.sub(r'\s+', '_', col.strip().lower()) not in dataFrame.columns]
                    
                    if len(columns_not_in_df) != 0:

                        print('The following columns are not in the data frame:', columns_not_in_df, '\n')

                    # get the columns from the data frame that are going to be repeated, the ones that are not in the configuration file list 
                    #base_columns_list = list(set([x.lower().replace(' ', '_') for x in columns_in_df]).symmetric_difference(set(dataFrame.columns)))
                    base_columns_list = list(set([re.sub(r'\s+', '_', x.lower()) for x in columns_in_df]).symmetric_difference(set(dataFrame.columns)))
                    
                    # extracting columns listed in columns_to_repeat from the data frame
                    base_df = dataFrame[base_columns_list]
                    
                    # Create a new data frame to store the transformed data
                    new_df = pd.DataFrame()

                    # Initialize tqdm progress bar with the total number of columns to transform
                    with tqdm(total=len(columns_in_df), desc="Transforming columns") as pbar:
                        
                        # transform columns that have measurement values into one single column 
                        for column in columns_in_df:
                        
                            # convert column name to lower case and remove spaces
                            column_lower = re.sub(r'\s+', '_', column.strip().lower())
                        
                            # split the column name into variable_name and variable_units (if present)
                            variable_name, variable_units = (lambda x: (x[0].strip().replace('_', ' ').lower().capitalize(), x[1].strip().replace(')', '')))(column.split('(')) if '(' in column else (column.strip().replace('_', ' ').lower().capitalize(), '')
                        
                             # Extract the variable values for the current column and rename the column
                            df_variable_value = dataFrame[[column_lower]].rename(columns={column_lower: variable_value_label})
                        
                            # Add variable_units column with the extracted units for each row
                            df_variable_value[variable_units_label] = [variable_units for _ in range(len(df_variable_value))]
                        
                            # Add variable_name column with the extracted variable name for each row
                            df_variable_value[variable_name_label] = [variable_name for _ in range(len(df_variable_value))]
                            
                            # Append the transformed columns to the new DataFrame
                            temp_df = pd.concat([base_df, df_variable_value], axis=1)
                        
                            # Append the transformed columns to the new DataFrame
                            new_df = pd.concat([new_df, temp_df])

                            # Update progress bar
                            pbar.update(1)

                    # Get the list of characters that are considered invalid for the column variable_value 
                    invalid_values_dict = functions.get_yml_item_value(additional_config_file, item_INVALID_VALUES)

                    invalid_values = []

                    for key in invalid_values_dict:
                        invalid_values.extend(invalid_values_dict[key])

                    df_before_row_del = len(new_df) # Length of the df before delete rows

                    # Delete the rows if the column variable_value is empty
                    new_df = new_df[new_df[variable_value_label].notna()]
                    
                    # Delete the rows if the column variable_value contains one of the characters contein in the invalid_values
                    new_df = new_df[~new_df[variable_value_label].isin(invalid_values)]                    
                    

                    df_after_row_del = len(new_df) # Length of the df after delete rows

                    # Create message if any row was deleted
                    if df_before_row_del - df_after_row_del > 0:

                        print(f'{df_before_row_del - df_after_row_del} empty VARIABLE VALUE ROWS were found in your data frame.\n')
                        print('EMPTY VARIABLE VALUE ROWS found were DELETED!\n')

                    # Display the new DataFrame
                    print('New dataframe head: \n')
                
                    print(new_df.head())

                    return new_df
                
                else:

                    print(f"The following values were provided: '{new_unique_columns}'\n")  
                    print('Three labels for the columns where expected: \n', 
                          ' - Variable values (first)\n', ' - Variable units (second)\n', ' - Variable name (third)\n') 

                    return dataFrame

            else: 

                print(f"No values provided in the '{item_NAMES_NEW_UNIQUE_COLUMNS}' block of the configuration file. No updates performed.\n")
            
                return dataFrame

        else:
            
            print(f"No values provided in the '{item_TRANSPOSE_COLUMNS_TO_ONE_COLUMN}' block of the  configuration file. No updates performed.\n")
            
            return dataFrame


def delete_duplicate_and_create_primary_key(config_et_file_name, item_CREATE_PRIMARY_KEY_IF_NEEDED, item_PRIMARY_KEY_COLUMN, dataFrame):
    """
    Generates a primary key for the DataFrame based on specified columns from the YAML configuration file.
    Handles duplicate rows to maintain primary key integrity.
    If no configuration values are provided, deletes duplicate rows from the DataFrame.

    Args:
        config_et_file_name (str): Path to the YAML configuration file.
        item_CREATE_PRIMARY_KEY_IF_NEEDED (str): Item identifier to retrieve keys and values from the YAML configuration.
        item_PRIMARY_KEY_COLUMN (str): Item identifier to retrieve the primary key column name from the YAML configuration.
        dataFrame (pandas.DataFrame): DataFrame containing the data to transform.

    Returns:
        pandas.DataFrame: DataFrame with the primary key column added or updated.
    """

    # Check if DataFrame is None
    if dataFrame is None:
        print("Error: The data frame provided is a Null object.\nPlease review if the previous function was well executed.")
        return

    # run the function that detects errors in the YAML file structure
    yaml_errors = functions.detect_yaml_errors(config_et_file_name)

    if yaml_errors is not None:
        # Handle the error in phrasing YML file
        print(yaml_errors)

        print('\nNO DATA FRAME WAS CREATED!')

    else:


        # retrieve primary key creation settings from the YAML configuration file
        block_keys = functions.get_yml_item_value(config_et_file_name, item_CREATE_PRIMARY_KEY_IF_NEEDED)

                      
        if block_keys != None:

            # get the name of the new column to create from the config file
            #name_column_id = [value for value in functions.get_yml_item_value(config_et_file_name, item_CREATE_PRIMARY_KEY_IF_NEEDED).keys()][0].lower()
            name_column_id = [re.sub(r'\s+', '_', value.lower()) for value in functions.get_yml_item_value(config_et_file_name, item_CREATE_PRIMARY_KEY_IF_NEEDED).keys()]
            
            # get the column names from the config file and make them lower case to create the id sample
            list_columns_id = [re.sub(r'\s+', '_', x.lower()) for x in
                                   [value for value in functions.get_yml_item_value(config_et_file_name, item_CREATE_PRIMARY_KEY_IF_NEEDED).values()][0]]

            # get a list of columns that exists in bot the list of config file and the dataframe
            columns_in_dataFrame = functions.matching_elements_two_lists(list_columns_id, dataFrame.columns.tolist())

            # create a temporary column to identify each sampling based on the specified columns
            dataFrame['temp_unique'] = dataFrame[columns_in_dataFrame].apply(lambda x: '-'.join(str(value) for value in x), axis = 1)

            # sort rows by columns in the list list_observ_names
            dataFrame = dataFrame.sort_values(by=columns_in_dataFrame)

            # empty list to store observation names
            list_observ_names = [] 
            
            # Initialize tqdm progress bar with the total number of columns to transform
            with tqdm(total=len(dataFrame['temp_unique'].unique()), desc="Creating the primary key column") as pbar:

                # for each unique value, create a list of characters to identify a unique observation when repeated. Store the characters in the list
                for unique in dataFrame['temp_unique'].unique():
        
                    list_observ_names.extend(functions.list_characters(len(dataFrame[dataFrame['temp_unique'] == unique])))
                    # Update progress bar
                    pbar.update(1)

            # create a new column using the list of characters
            dataFrame['observation_name'] = list_observ_names

            # delete the temporal column
            del dataFrame['temp_unique']

            # add observation_name to list of list_columns_id
            list_columns_id.insert(len(list_columns_id)-1, 'observation_name')
            
            # create id_observation column 
            dataFrame[name_column_id[0]] = dataFrame[list_columns_id].apply(lambda x: '-'.join(str(value) for value in x), axis = 1)

            # review if there are any duplicated rows
            num_duplicates_before = len(dataFrame)
            
            # Drop duplicated rows with the same values in all columns
            no_dup_row_dataFrame = dataFrame.drop_duplicates(keep='first', ignore_index = True)
            
            num_duplicates_after = len(no_dup_row_dataFrame)

            num_duplicates_deleted = num_duplicates_before - num_duplicates_after

            # Delete duplicated rows in all columns
            if num_duplicates_deleted > 0:
                print(f'\n{num_duplicates_deleted} duplicate rows were found in your data frame.\n')
                print('DUPLICATED ROWS found were DELETED!\n')

            # validate that the primary key has a unique value
            if no_dup_row_dataFrame[name_column_id[0]].is_unique:

                print(f"\nThe column '{name_column_id[0]}' was created successfully.\n")

                print('New dataframe head: \n')
                
                print(no_dup_row_dataFrame.head())

                return no_dup_row_dataFrame

            else: 
                
                print('\nColumns indicated in the CREATE_PRIMARY_KEY_IF_NEEDED block does not create a unique identifier column!\n')

                print('The function will return the Data Frame WITHOUT a primary key column')
                
                print('Some examples repeated rows are:\n')

                duplicated_rows = dataFrame[dataFrame.duplicated([name_column_id[0]])==True]
                
                print(duplicated_rows.head())
                   
        else:

            # Delete duplicated rows in all columns
            if dataFrame.duplicated().any():

                num_duplicates_before = len(dataFrame)
                
                # Drop duplicated rows with the same values in all columns
                dataFrame = dataFrame.drop_duplicates(keep='first', ignore_index = True)
                
                num_duplicates_after = len(new_dataFrame)
                
                num_duplicates_deleted = num_duplicates_before - num_duplicates_after

                print(f'{num_duplicates_deleted} duplicate rows were found in your data frame.\n')
                print('DUPLICATED ROWS found were DELETED!\n')

            print("No values provided for the CREATE_PRIMARY_KEY_IF_NEEDED block in the configuration file. No updates performed.")

            print('Dataframe info: \n')
                
            print(dataFrame.info())

            return dataFrame


def update_primary_key_values(config_et_file_name, item_UPDATE_PRIMARY_KEY_VALUES, item_PRIMARY_KEY_COLUMN, dataFrame):
    """
    Update parts of the primary key values in the specified DataFrame based on the values provided in the configuration file. 
    It replaces specified substrings in the primary key column with new values.

    Args:
        config_et_file_name (str): Configuration file name.
        item_UPDATE_PRIMARY_KEY_VALUES (str): Key in the configuration file containing the values to update.
        item_PRIMARY_KEY_COLUMN (str): Key in the configuration file specifying the primary key column.
        dataFrame (pandas.DataFrame): DataFrame to update.

    Returns:
        pandas.DataFrame: Updated DataFrame.
    """

    # Check if DataFrame is None
    if dataFrame is None:
        print("Error: The data frame provided is a Null object.\nPlease review if the previous function was well executed.")
        return

    # run the function that detects errors in the YAML file structure
    yaml_errors = functions.detect_yaml_errors(config_et_file_name)

    if yaml_errors is not None:
        # Handle the error in phrasing YML file
        print(yaml_errors)

        print('\nNO DATA FRAME WAS CREATED!')

    else:

        # primary key values 
        primary_key_values = functions.get_yml_item_value(config_et_file_name, item_UPDATE_PRIMARY_KEY_VALUES)

        if primary_key_values != None:

            # get primary key
            primary_key = functions.get_yml_item_value(config_et_file_name, item_PRIMARY_KEY_COLUMN)[0]

            # Check if primary key column exists in the DataFrame
            if primary_key not in dataFrame.columns:

                print(f"Error: Primary key column '{primary_key}' not found in DataFrame columns.")

            else:

                # Initialize tqdm progress bar with the total number of keys to transform
                with tqdm(total=len(primary_key_values.items()), desc="Updating key") as pbar:

                    for key, value in primary_key_values.items():

                        dataFrame[primary_key] = dataFrame[primary_key].str.replace(key, value)

                        pbar.update(1)

                print('New dataframe head: \n')
                    
                print(dataFrame.head())
            
                return dataFrame

        else:

            print("No values provided in the configuration file. No updates performed.")

            return dataFrame
    


def export_dataframe(config_et_file_name, item_OUTPUT_FILE_NAME, dataFrame):   
    """
    This function exports the provided DataFrame to a CSV file based on the configuration specified in the configuration file.

    Args:
        config_et_file_name (str): Configuration file name.
        item_OUTPUT_FILE_NAME (str): Key for output file name in the configuration.
        dataFrame (pandas.DataFrame): DataFrame to export.

    Returns:
        None
    """
    # Check if DataFrame is None
    if dataFrame is None:
        print("Error: Error: The data frame provided is a Null object.\nPlease review if the previous function was well executed.\nThe file can not be exported! .")
        return

    # run the function that detects errors in the YAML file structure
    yaml_errors = functions.detect_yaml_errors(config_et_file_name)

    if yaml_errors is not None:
        # Handle the error in phrasing YML file
        print(yaml_errors)

        print('\nNO DATA FRAME WAS CREATED!')

    else:

        output_file_name = functions.get_yml_item_value(config_et_file_name, item_OUTPUT_FILE_NAME)

        # get time and add it to the output file 
        timestr = time.strftime("%Y%m%d-%H%M%S")

        if output_file_name == None:

            output_file_name = timestr + '_et_output_file'

            functions.save_dataFrame_to_csv(dataFrame, './et_output/', output_file_name)

        else:

            output_file_name = timestr + '_' + output_file_name[0]

            functions.save_dataFrame_to_csv(dataFrame, './et_output/', output_file_name)


def database_connection(config_load_file_name, item_DATABASE_CREDENTIALS):
    '''
    Establishes a connection to the database using the credentials provided in the DATABASE_CREDENTIALS section of the config_load.yml file.
    
    Args:
        config_load_file_name (str): Name of the configuration file.
        item_DATABASE_CREDENTIALS (str): Key specifying the database credentials in the configuration file.
    
    Returns:
        connection (psycopg2.extensions.connection): Connection object to the PostgreSQL database.
        cursor (psycopg2.extensions.cursor): Cursor object for executing SQL commands.
    '''

    # read information from config file
    database_credentials = functions.get_yml_item_value(config_load_file_name, item_DATABASE_CREDENTIALS)

    # num to check if it is the firsts value of a key
    num = len(database_credentials)

    # Prompt the user to enter the password
    password = getpass.getpass("Enter the database password: ")
    
    # get values of conection database into a string
    for key in database_credentials:

        if num == len(database_credentials):

            DB_CONNECTION_STRING = key.lower() + '=%s ' % database_credentials[key] 

        else:

            DB_CONNECTION_STRING = DB_CONNECTION_STRING + key.lower() + '=%s ' % database_credentials[key]

        num -= 1

    # add the password to the string for the db conection 
    DB_CONNECTION_STRING = DB_CONNECTION_STRING + 'password=' + password

    ### Open database conection
    try: 
        connection = psycopg2.connect(DB_CONNECTION_STRING)

        print("Conection created!")

    except psycopg2.Error as e: 

        print("Error: Could not make connection to the Postgres database")
        print(e)

    try: 
        cursor = connection.cursor()
        print("Cursor obtained!")

    except psycopg2.Error as e: 
        
        print("Error: Could not get cursor")
        print(e)

    connection.set_session(autocommit=True)
    
    return connection, cursor
    

def execute_sql_statement(cursor, connection, sql_statement):
    '''
    Executes SQL statements on the database using the provided cursor.
    
    Args:
        cursor (psycopg2.extensions.cursor): Cursor object for executing SQL commands.
        connection (psycopg2.extensions.connection): Connection object to the PostgreSQL database.
        sql_statement (str or list): SQL statement(s) to execute.
    '''
    
    for query in sql_statement:
        
        try:
            
            cursor.execute(query)
            print("Query executed successfully!\n")
            
        except psycopg2.Error as e: 
            
            print("Error: issue executing sql statement")
            print (e)
            
        connection.commit()


def sql_statement_create_table_if_not_exists(config_load_file_name, item_NEW_TABLE_COLUMNS, item_TABLE_NAME):
    '''
    Generates an SQL statement to create a new table in the database if it does not already exist.
    
    Args:
        config_load_file_name (str): Name of the configuration file.
        item_NEW_TABLE_COLUMNS (str): Key specifying the new table columns in the configuration file.
        item_TABLE_NAME (str): Key specifying the table name in the configuration file.
    
    Returns:
        str: SQL statement to create the new table.
    '''

    # read columns to create new table from config file
    new_table_columns = functions.get_yml_item_value(config_load_file_name, item_NEW_TABLE_COLUMNS)

    # get the table name
    table_name = functions.get_yml_item_value(config_load_file_name, item_TABLE_NAME)[0].lower()
    
    if new_table_columns != None:

        # num to check if it is the firsts value of a key
        num = len(new_table_columns)

        # get values columns to build the sql statement to create the new table 
        for key in new_table_columns:

            if num == len(new_table_columns) and num == 1:
                
                new_table_statement = ' CREATE TABLE IF NOT EXISTS ' + table_name + ' (' + key.lower() + ' %s\n' % new_table_columns[key] + '); '

            elif num == len(new_table_columns):
                
                new_table_statement = ' CREATE TABLE IF NOT EXISTS ' + table_name + ' (' + key.lower() + ' %s,\n' % new_table_columns[key] + '\t'

                
            elif num == 1:

                #new_table_statement = new_table_statement + key.lower() + ' %s,\n' % new_table_columns[key] + ');""" '

                new_table_statement = new_table_statement + key.lower() + ' %s\n' % new_table_columns[key] + '); '

            else:

                new_table_statement = new_table_statement + key.lower() + ' %s,\n' % new_table_columns[key] + '\t'

            num -= 1

        new_table_statement = new_table_statement.rstrip(new_table_statement[-1])

        return new_table_statement
    
    else:

        return print('No columns were found in the configuration file!')


def print_columns(cursor, config_load_file_name, item_TABLE_NAME):
    '''
    Retrieves and prints column names and data types for a given table.
    
    Args:
        cursor (psycopg2.extensions.cursor): Cursor object for executing SQL commands.
        config_load_file_name (str): Name of the configuration file.
        item_TABLE_NAME (str): Key specifying the table name in the configuration file.
    '''
    
    table_name = functions.get_yml_item_value(config_load_file_name, item_TABLE_NAME)[0].lower()

    sql = 'SELECT column_name, ordinal_position, is_nullable, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = ' + "'" + table_name  + "'" + 'ORDER BY ordinal_position'
    
    cursor.execute(sql)

    columns = cursor.fetchall()
    
    print('---------------------------------------')
    
    print('Table: ' + table_name)
    
    print('---------------------------------------')
    
    print('column_name | data_type')
    
    print('---------------------------------------')    

    for row in columns:

        print("{}".format(row[0]) + " | {}".format(row[3]))


def sql_statement_add_column_if_table_exists(config_load_file_name, item_NEW_COLUMNS_IF_TABLE_EXISTS, item_TABLE_NAME):
    '''
    Generates an SQL statement to add new columns to an existing table if it exists.
    
    Args:
        config_load_file_name (str): Name of the configuration file.
        item_NEW_COLUMNS_IF_TABLE_EXISTS (str): Key specifying the new columns in the configuration file.
        item_TABLE_NAME (str): Key specifying the table name in the configuration file.
    
    Returns:
        str: SQL statement to add new columns to the existing table.
    '''

    # read columns to create new table from config file
    new_columns = functions.get_yml_item_value(config_load_file_name, item_NEW_COLUMNS_IF_TABLE_EXISTS)

    # get the table name
    table_name = functions.get_yml_item_value(config_load_file_name, item_TABLE_NAME)[0].lower()

    # get values columns to build the sql statement to create the new table 

    if new_columns != None:

        # num to check if it is the firsts value of a key
        num = len(new_columns)

        for key in new_columns:

            if num == len(new_columns) and num == 1:

                new_column_statement = ' ALTER TABLE ' + table_name +  ' ADD COLUMN IF NOT EXISTS ' + key.lower() + ' %s\n' % new_columns[key] + '; '
                
            elif num == len(new_columns):
                
                 new_column_statement = ' ALTER TABLE ' + table_name +  ' ADD COLUMN IF NOT EXISTS ' + key.lower() + ' %s,\n' % new_columns[key]
                
            elif num == 1:

                new_column_statement = new_column_statement + ' ADD COLUMN IF NOT EXISTS ' + key.lower() + ' %s\n' % new_columns[key] + '; '

            else:

                new_column_statement = new_column_statement + ' ADD COLUMN IF NOT EXISTS ' + key.lower() + ' %s,\n' % new_columns[key] 

            num -= 1
        
        new_column_statement = new_column_statement.rstrip(new_column_statement[-1])

        return new_column_statement

    else:

        return print('No new columns were found in the configuration file!')



def describe_table(cursor, table_name):
    '''
    Retrieves and prints the structure of a given table.
    
    Args:
        cursor (psycopg2.extensions.cursor): Cursor object for executing SQL commands.
        table_name (str): Name of the table.
    '''

    try:

        sql = 'SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ' + "'" + table_name  + "'" + ' ORDER BY ordinal_position'

        cursor.execute(sql)

        columns = cursor.fetchall()

        print('---------------------------------------')

        print('Table: ' + table_name)

        print('---------------------------------------')

        print('column_name | data_type')

        print('---------------------------------------')    

        for row in columns:

            print("{}".format(row[0]) + " | {}".format(row[1]))

    except psycopg2.Error as e: 
            
        print("Error: issue executing sql statement")
        print (e)


def insert_dataframe_to_database(connection, cursor, config_load_file_name, item_FILE_TO_UPLOAD, item_TABLE_NAME, item_PRIMARY_KEY_COLUMN):
    '''
    Inserts the contents of a pandas DataFrame into a PostgreSQL table.
    
    Args:
        connection (psycopg2.extensions.connection): Connection object to the PostgreSQL database.
        cursor (psycopg2.extensions.cursor): Cursor object for executing SQL commands.
        config_load_file_name (str): Name of the configuration file.
        item_FILE_TO_UPLOAD (str): Key specifying the file to upload in the configuration file.
        item_TABLE_NAME (str): Key specifying the table name in the configuration file.
        item_PRIMARY_KEY_COLUMN (str): Key specifying the primary key column in the configuration file.
    '''

    # get the table name
    table = functions.get_yml_item_value(config_load_file_name, item_TABLE_NAME)[0].lower()

    # get primary key
    primary_key = functions.get_yml_item_value(config_load_file_name, item_PRIMARY_KEY_COLUMN)[0].lower()

    # get file to upload using the csv_file_to_df() function
    dataframe = functions.csv_file_to_df(config_load_file_name, item_FILE_TO_UPLOAD)

    # get name of the data file to upload
    values_template_imput = functions.get_yml_item_value(config_load_file_name, item_FILE_TO_UPLOAD).values()

    if any(s.startswith('./') and s.endswith('/') for s in values_template_imput):

        if any(s.endswith('.csv') for s in values_template_imput):

            for value in values_template_imput:

                if os.path.isdir(value):

                    folder_path = value

                else:

                    file_name = value

    # Convert "NaT" values in the "date" column to None
    dataframe['date'] = dataframe['date'].apply(lambda x: None if pd.isna(x) else x)

    # Convert NaN values to None in the entire DataFrame
    dataframe = dataframe.where(pd.notnull(dataframe), None)

    # Convert DataFrame to list of tuples
    tuples = [tuple(x) for x in dataframe.to_numpy()]
  
    columns = ','.join(list(dataframe.columns))
    
    # create list of virtual EXCLUDED table 
    columns_excluded = ['excluded.' + column for column in dataframe.columns]

    columns_excluded = ','.join(columns_excluded)
    
    # SQL query to execute
    sql_insert = 'INSERT INTO %s(%s) VALUES %%s' % (table, columns) + ' ON CONFLICT (' + primary_key +') DO UPDATE'
    
    sql_set = ' SET (%s)' % (columns) + ' = ' + '(%s)' % (columns_excluded)
    
    sql = sql_insert + sql_set
    
    try:
        
        extras.execute_values(cursor, sql, tuples)
        
        connection.commit()

        return print(f"The data of the {file_name} file has been inserted successfully into the database!\n")
        
    except (Exception, psycopg2.DatabaseError) as error:
        
        connection.rollback()
        
        return print("\nError: %s" % error)