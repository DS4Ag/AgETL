import pandas as pd
import os
import errno
import yaml

# import functions file
import sys
file = 'functions.py'
sys.path.insert(0,os.path.dirname(os.path.abspath(file)))
import functions


'''
Extract process

'''
def configuration_data():
    
    config_et_file_name = 'config_et.yml'

    item_ADDITIONAL_INFORMATION_FILES = 'ADDITIONAL_INFORMATION_FILES'

    item_FILES_TO_PROCESS = 'FILES_TO_PROCESS'

    item_JOIN_FILES_COMMON_COLUMNS = 'JOIN_FILES_COMMON_COLUMNS'

    item_COLUMNS_TO_DROP = 'COLUMNS_TO_DROP'

    item_VALUES_TRANSFORM = 'VALUES_TRANSFORM'

    item_NEW_COLUMNS = 'NEW_COLUMNS'

    item_OUTPUT_FILE_NAME = 'OUTPUT_FILE_NAME'

    item_CHANGE_NAME_COLUMNS = 'CHANGE_NAME_COLUMNS'
    
    return config_et_file_name, item_ADDITIONAL_INFORMATION_FILES, item_FILES_TO_PROCESS, item_JOIN_FILES_COMMON_COLUMNS, item_COLUMNS_TO_DROP, item_VALUES_TRANSFORM,  item_NEW_COLUMNS, item_OUTPUT_FILE_NAME, item_CHANGE_NAME_COLUMNS


def extract_and_join_files(config_et_file_name, item_FILES_TO_PROCESS, item_ADDITIONAL_INFORMATION_FILES, 
                       item_JOIN_FILES_COMMON_COLUMNS):
    ### Extract and join files to process
    # FILES_TO_PROCESS

    # get and concatenate all the files that are going to be proccessed. Function concatenate_csv_files() is used
    data_frame_to_process = functions.concatenate_csv_files(config_et_file_name, item_FILES_TO_PROCESS)

    ### If there is additional information to add, join to the dataframe. If not only change the name of the data frame
    # ADDITIONAL_INFORMATION_FILES
    # JOIN_FILES_COMMON_COLUMNS

    additional_information_files = functions.get_yml_item_value(config_et_file_name, item_ADDITIONAL_INFORMATION_FILES)

    if additional_information_files != None:

        # get and concatenate all the Experiment info files that are going to be proccessed. Function concatenate_csv_files() is used
        experiment_info_files = functions.concatenate_csv_files(config_et_file_name, item_ADDITIONAL_INFORMATION_FILES)

        # create a list whit the name of the dataFames
        dataFrame_list = [experiment_info_files, data_frame_to_process]

        # get the list of columns that are going to be used to join the dataframes
        columns_join = functions.get_yml_item_value(config_et_file_name, item_JOIN_FILES_COMMON_COLUMNS)
        columns_join_lower = [s.lower().replace(' ', '') for s in columns_join]

        for dataFrame in dataFrame_list:

            # check if the columns to be used to join the dataframes that are writen in the config file exist in 
            # dataframe
            set_of_columns = functions.check_if_list_columns_exist_in_dataframe(columns_join_lower, dataFrame)

            if isinstance(set_of_columns, set):

                print(f"Columns: {', '.join(set(columns_join_lower).difference(dataFrame.columns))} does not exist in the dataframe! Please fix it!")

                break 

        joined_dataFrame = pd.merge(experiment_info_files, data_frame_to_process, on = columns_join_lower, how = 'left') 

    else:

        joined_dataFrame = data_frame_to_process

    print(joined_dataFrame.info())
        
    return joined_dataFrame


'''
Transform process

'''
def drop_not_used_columns(config_et_file_name, item_COLUMNS_TO_DROP, extract_and_join_files):
    ### Drop unused columns
    # COLUMNS_TO_DROP
    
    dataFrame = extract_and_join_files

    # get the list of columns that are going to be used to join the dataframes
    columns_to_drop = functions.get_yml_item_value(config_et_file_name, item_COLUMNS_TO_DROP)

    # if there is any value in the config file to drop, run the following code to drop the listed columns
    if not isinstance(columns_to_drop, type(None)):

        columns_to_drop_lower = [s.lower().replace(' ', '') for s in columns_to_drop]

        columns_to_drop_lower

        # drop columns from the dataframe 
        dataFrame = dataFrame.drop(columns = columns_to_drop_lower)

    print(dataFrame.info())

    return dataFrame


def update_column_values(config_et_file_name, item_UPDATE_COLUMN_VALUES, drop_not_used_columns):
    ### Update values column by column 
    # VALUES_TRANSFORM
    
    dataFrame = drop_not_used_columns

    ### Update row values 

    # get dict values from config file
    values_update =  functions.get_yml_item_value(config_et_file_name, item_UPDATE_COLUMN_VALUES)

    if values_update != None:

        # get values and keys from each dictionary element
        for key in values_update:

            value = values_update[key]

            # review each column to update the values    
            for column in dataFrame.columns:

                #print(joined_dataFrame[column])

                dataFrame[column].mask(dataFrame[column] == key, value, inplace=True)
                
    print(dataFrame.head())
        
    return dataFrame


def add_new_columns(config_et_file_name, item_NEW_COLUMNS, update_column_values): 
    ### Add new columns to the dataFrame 
    # NEW_COLUMNS

    dataFrame = update_column_values
    
    # new columns to add 
    new_columns = functions.get_yml_item_value(config_et_file_name, item_NEW_COLUMNS)

    if new_columns != None:

        number = len(dataFrame.columns)  # it will be used to get the dataframe position of new columns

        for key_column in new_columns:

            dataFrame.insert(number, key_column.lower(), new_columns.get(key_column))

            number += 1
            
    print(dataFrame.info())
            
    return dataFrame


def update_column_names(config_et_file_name, item_UPDATE_COLUMN_NAMES, add_new_columns):
    ### Update column names
    # UPDATE_COLUMN_NAMES
    
    dataFrame = add_new_columns
    
    # new columns to add 
    update_column_names = functions.get_yml_item_value(config_et_file_name, item_UPDATE_COLUMN_NAMES)

    if update_column_names != None:

        for key, value in update_column_names.items():

            dataFrame.rename({key.lower() : value.lower()}, axis=1, inplace=True)
            
    print(dataFrame.info())
            
    return dataFrame

def export_dataframe(config_et_file_name, item_OUTPUT_FILE_NAME, update_column_names):   
    ### Export the new file created
    # UTPUT_FILE_NAME
    
    dataFrame = update_column_names 

    output_file_name = functions.get_yml_item_value(config_et_file_name, item_OUTPUT_FILE_NAME)

    if output_file_name == None:

        functions.save_dataFrame_to_csv(dataFrame, 'output_file')

    else:

        functions.save_dataFrame_to_csv(dataFrame, output_file_name[0])