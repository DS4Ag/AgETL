import pandas as pd
import os
import errno
import yaml

def get_yml_item_value(file, item_input):
# function that opens the yaml file and returns the value that the item has

    # transform the argument values into lowcase or uppercase
    file = file.lower()
    item_input = item_input.upper()

    with open(file, 'r') as file:

        configuration = yaml.full_load(file)

        for item, value in configuration.items():

            if item == item_input:
                value_output = value

                return value_output


def csv_file_to_df(file, item_input):
# function that reads a csv file using config file that it name and path location is declared in a YAML config file.

    # get the path of the template folder from the YAML config file
    values_template_imput = get_yml_item_value(file, item_input).values()

    if any(s.startswith('./') and s.endswith('/') for s in values_template_imput):

        if any(s.endswith('.csv') for s in values_template_imput):

            for value in values_template_imput:

                if os.path.isdir(value):

                    folder_path = value

                else:

                    file_name = value

            name_file_read = folder_path + file_name

            # read the base template
            base_template = pd.read_csv(name_file_read)

            # transform column names into lowcase to make them case insensitive
            base_template.columns = base_template.columns.str.lower()

            return base_template

        else:
            print('ERROR! = File name does not have .CSV extension!')

    else:

        print('ERROR! = Path should be like: ./folder/')


def concat_list_csv_files(list_of_csv_files):
# function that concatenates a list of detaframes
    
    num = 0
    
    for file in list_of_csv_files:

        if num == 0:

            dataFrame = pd.read_csv(file) 
            
        else:

            temp_dataFrame = pd.read_csv(file)

            dataFrame = pd.concat([dataFrame, temp_dataFrame])

        num =+ 1

    # reset index
    dataFrame.reset_index(inplace=True, drop=True) 
    
    # transform column names into lower case
    
    dataFrame_columns_lower = [s.lower().replace(' ', '') for s in dataFrame.columns]
            
    dataFrame.columns = dataFrame_columns_lower
    
    return dataFrame


def concatenate_csv_files_updating_column_names(file, item_input, item_UPDATE_COLUMN_NAMES):
# function that updates column names and concatenates a list of detaframes

    # get the column names items to change using the get_yml_item_value() function
    update_column_names = get_yml_item_value(file, item_UPDATE_COLUMN_NAMES)

    # get the path of the files to changge column names and join the data frames
    dir_path = get_path_item(file, item_input)

    num = 0

    # update column names from each file once they are oppened
    for file in os.listdir(dir_path):

        # if is it is a file
        if os.path.isfile(os.path.join(dir_path, file)):

            path = dir_path + file

            # if it is the first dataframe
            if num == 0:

                dataFrame = pd.read_csv(path)

                # if item UPDATE_COLUMN_NAMES have content
                if update_column_names != None:

                    for key, value in update_column_names.items():
                        # make all column names lowcase
                        dataFrame.columns = dataFrame.columns.str.lower()

                        # replace the column name if it is in the item UPDATE_COLUMN_NAMES
                        dataFrame.rename({key.lower(): value.lower()}, axis=1, inplace=True)

            else:

                temp_dataFrame = pd.read_csv(path)

                # if item UPDATE_COLUMN_NAMES have content
                if update_column_names != None:

                    for key, value in update_column_names.items():
                        # make all column names lowcase
                        temp_dataFrame.columns = temp_dataFrame.columns.str.lower()

                        # replace the column name if it is in the item UPDATE_COLUMN_NAMES
                        temp_dataFrame.rename({key.lower(): value.lower()}, axis=1, inplace=True)

                dataFrame = pd.concat([dataFrame, temp_dataFrame])

            num = + 1

    # reset index
    dataFrame.reset_index(inplace=True, drop=True)

    # transform column names into lower case
    dataFrame_columns_lower = [s.lower().replace(' ', '') for s in dataFrame.columns]

    dataFrame.columns = dataFrame_columns_lower

    return dataFrame


def save_dataFrame_to_csv(dataFrame, path ,save_file_name):
# create the folder where the output file will be stored
    try:
        os.makedirs(path)

    except OSError as e:

        if e.errno != errno.EEXIST:

            raise

    # create and save the file
    dataFrame.to_csv(path + save_file_name + '.csv', index=False)

    print('Dataframe: ' + path + save_file_name + '.csv' + ' created successfully!')  


def get_path_item(file, item_input):
# function that returns a path and a list of values from the declareditem in a configuration file of the form:
# BLOCK COLLECTION 
#  Key: ./folder_path/
#  Key: 
#    - Sequences

    values_list_items = get_yml_item_value(file, item_input).values()

    for value in values_list_items:
        
        # get the list of sequences and store them in a list
        if isinstance(value, type(list)):

            value_list = value


        else:
            
            # get the path and store it in a variable

            if any(s.startswith('./') and s.endswith('/') for s in values_list_items):

                if os.path.isdir(value):

                    folder_path = value

            else:

                print('ERROR! = Path should be like: ./input/') 
                
    # return the path and the list of sequences
    
    return folder_path


def concatenate_csv_files(file, item_input):
# function that reads and concatenates a list of files that exist in a directory

    # get the path where the files to concatenate are stored using the get_path_item() function 
    dir_path = get_path_item(file, item_input)

    # list to store files
    list_stored_files = []

    # Checking if the list is empty or not
    if not os.listdir(dir_path):

        print('Directory ' + dir_path + ' is empty')

    else:   

        # Iterate directory
        for file in os.listdir(dir_path):

            # check if current path is a file
            if os.path.isfile(os.path.join(dir_path, file)):

                path = dir_path + file

                list_stored_files.append(path)

        # concatenate files stored in the directory using the concat_list_csv_files() function
        dataFrame_concatenated_files = concat_list_csv_files(list_stored_files)
        
        return dataFrame_concatenated_files


def check_if_list_columns_exist_in_dataframe(list_columns, data_frame_name):
# function that checks if all column names declared on the configuration to join the data frames exist in them  
    
    # returns the a set of the columns that does not exist
    if not set(list_columns).issubset(set(data_frame_name.columns)):
    
        return set(list_columns).difference(data_frame_name.columns)
    
    return [0]


def create_table_sql_statement(cursor, conn, sql_statement):
### function that executes the sql_staement

    for statement in sql_statement:
        
        try:
            
            cursor.execute(statement)
            
        except psycopg2.Error as e: 
            
            print("Error: Issue executing SQL statement")
            
            print(e)
            
        conn.commit()