import pandas as pd
import os
import errno
import yaml
from string import ascii_uppercase
import itertools
import chardet
import re
from tqdm import tqdm
from yaml.scanner import ScannerError
import io

def detect_yaml_errors(file):
    """
    Detects errors in a YAML file and returns an error message if any.

    Args:
        file (str): Path to the YAML file.

    Returns:
        str or None: Error message if an error is detected, otherwise None.

    Raises:
        FileNotFoundError: If the specified YAML file does not exist.
        yaml.YAMLError: If an error occurs during parsing the YAML file.
        Exception: If any other unexpected error occurs.
    """
    try:
        # Load the YAML file
        with open(file, 'r') as f:
            yaml_content = yaml.safe_load(f)
        # If the YAML content is successfully loaded, return None (no error)
        return None
    except yaml.YAMLError as exc:
        # If an error occurs during parsing, return the error message
        return f"Error parsing YAML file: {exc}"
    except Exception as e:
        # Handle other exceptions
        return f"Error: {e}"


def get_yml_item_value(file, item_input):
    """
    Opens a YAML file and returns the value corresponding to the given item.

    Args:
        file (str): YAML file name.
        item_input (str): Item to search for in the YAML file.

    Returns:
        value_output: Value corresponding to the specified item.
    """

    # transform the argument values into lowcase or uppercase

    file = file.lower()
    item_input = item_input.upper()

    # Use chardet to detect encoding
 
    with open(file, 'rb') as raw_file:

        result = chardet.detect(raw_file.read())

    encoding = result['encoding']


    # Open the YAML file and get the values
    with open(file, 'r', encoding = encoding) as file:

        configuration = yaml.full_load(file)

        for item, value in configuration.items():

            if item == item_input:
                value_output = value

                return value_output


def csv_file_to_df(file, item_input):
    """
    Function to read a CSV file using path information provided by a YAML config file.

    Args:
        File (str): YAML file name
        item_input (str): The item identifier to retrieve from the YAML config.

    Returns:
        pandas.DataFrame: DataFrame containing the content of the CSV file, with column names transformed to lowercase for case insensitivity.

    Raises:
        ValueError: If the YAML config file does not contain valid path information.
        FileNotFoundError: If the specified CSV file does not exist.
        IOError: If an error occurs while reading the CSV file.
    """

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
    """
    Concatenates a list of DataFrames read from CSV files into a single DataFrame.

    Args:
        list_of_csv_files (list): A list of file paths to CSV files.

    Returns:
        pandas.DataFrame: DataFrame containing the concatenated data from all CSV files.
    """
    
    num = 0
    
    for file in list_of_csv_files:

        if num == 0:

            dataFrame = pd.read_csv(file) 
            
        else:

            temp_dataFrame = pd.read_csv(file)

            dataFrame = pd.concat([dataFrame, temp_dataFrame])

        num += 1

    dataFrame.reset_index(inplace=True, drop=True) 
    
    # transform column names into lower case and replace spaces
    dataFrame_columns_lower = [re.sub(r'\s+', '_', s.strip().lower()) for s in dataFrame.columns]
            
    dataFrame.columns = dataFrame_columns_lower
    
    return dataFrame


def concatenate_csv_files_updating_column_names(file, item_input, item_UPDATE_COLUMN_NAMES):
    """
    Concatenates CSV files, updating column names as specified in a YAML configuration.

    Args:
        file (str): Path to the YAML config file.
        item_input (str): Item identifier specifying the directory containing CSV files.
        item_UPDATE_COLUMN_NAMES (str): Item identifier specifying column name updates in the YAML config.

    Returns:
        pandas.DataFrame: DataFrame containing concatenated data from CSV files with updated column names.

    Raises:
        ValueError: If the YAML config file does not contain valid path or column name update information.
        FileNotFoundError: If the specified directory containing CSV files does not exist.
        IOError: If an error occurs while reading or concatenating the CSV files.
    """

    # get the column names items to change using the get_yml_item_value() function
    update_column_names = get_yml_item_value(file, item_UPDATE_COLUMN_NAMES)

    # get the path of the files to changge column names and join the data frames
    dir_path = get_path_item(file, item_input)

    # get the names of files to process 
    files = [f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))]
    print('Files to process: ')
    print(files)  # Print filenames only

    # count the number of files to process
    num_files = len([f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))])  # Total number of files

    num = 0

    # Create a tqdm progress bar for the loop
    with tqdm(total=num_files, desc="Processing files") as pbar:

        # update column names from each file once they are oppened
        for file in os.listdir(dir_path):

            # if is it is a file
            if os.path.isfile(os.path.join(dir_path, file)):

                path = dir_path + file

                # detect encoding

                with open(path, 'rb') as f:
                    
                    result = chardet.detect(f.read())
                
                encoding = result['encoding']

                # / detect encoding

                # if it is the first dataframe
                if num == 0:

                    dataFrame = pd.read_csv(path, encoding=encoding, index_col=False)

                    # if item UPDATE_COLUMN_NAMES have content
                    if update_column_names != None:

                        for key, value in update_column_names.items():
                            # make all column names lowcase
                            dataFrame.columns = dataFrame.columns.str.lower()

                            # replace the column name if it is in the item UPDATE_COLUMN_NAMES
                            dataFrame.rename({key.lower(): value.lower()}, axis=1, inplace=True)

                else:

                    temp_dataFrame = pd.read_csv(path, encoding=encoding, index_col=False)

                    # if item UPDATE_COLUMN_NAMES have content
                    if update_column_names != None:

                        for key, value in update_column_names.items():
                            # make all column names lowcase
                            temp_dataFrame.columns = temp_dataFrame.columns.str.lower()

                             # replace the column name if it is in the item UPDATE_COLUMN_NAMES
                            temp_dataFrame.rename({key.lower(): value.lower()}, axis=1, inplace=True)

                    # Reset index of both dataFrame and temp_dataFrame
                    dataFrame.reset_index(drop=True, inplace=True)

                    temp_dataFrame.reset_index(drop=True, inplace=True)
                    
                    dataFrame = pd.concat([dataFrame, temp_dataFrame])
                         
                num = + 1

                # Update progress bar
            pbar.update(1)

    # reset index
    dataFrame.reset_index(inplace=True, drop=True)

    # transform column names into lower case and replace empty spaces
    dataFrame_columns_lower = [re.sub(r'\s+', '_', s.strip().lower()) for s in dataFrame.columns]

    dataFrame.columns = dataFrame_columns_lower

    return dataFrame


def save_dataFrame_to_csv(dataFrame, path ,save_file_name):
    """
    Saves a DataFrame to a CSV file at the specified path.

    Args:
        dataFrame (pandas.DataFrame): DataFrame to be saved.
        path (str): Directory path where the CSV file will be stored.
        save_file_name (str): Name of the CSV file to be saved (without the ".csv" extension).

    Returns:
        None

    Raises:
        OSError: If an error occurs while creating the directory.
        IOError: If an error occurs while saving the DataFrame to CSV.
    """

    # create the folder where the output file will be stored
    try:
        os.makedirs(path)

    except OSError as e:

        if e.errno != errno.EEXIST:

            raise

    # create and save the file
    dataFrame.to_csv(path + save_file_name + '.csv', index = False)

    print('Dataframe: ' + path + save_file_name + '.csv' + ' created successfully!')  


def get_path_item(file, item_input):
    """
    Returns a path and a list of values from the declared item in a configuration file.

    Args:
        file (str): Path to the configuration YAML file.
        item_input (str): Item to search for in the YAML file.

    Returns:
        tuple: A tuple containing the directory path and an error message if an error occurs,
               or (None, None) if no error occurs.
    """

    # get the value from YAML file
    yaml_value = get_yml_item_value(file, item_input)

    # extract values_list_items from the obtained yaml_value
    values_list_items = yaml_value.values()

    for value in values_list_items:
        
        # get the list of sequences and store them in a list
        if isinstance(value, type(list)):

            value_list = value

        else:
            
            # get the path and store it in a variable
            if any(s.startswith('./') and s.endswith('/') for s in values_list_items):
                
                # review if the path exists as directory
                if os.path.isdir(value):

                    folder_path = value
                    
                    # return the path and the list of sequences
                    return folder_path
                    
                else:
                    
                    error_msg = f'The directory {value} does not exist'
                    print(error_msg)

            else:

                error_msg = 'ERROR! Path should be like: ./input/'
                print(error_msg)
                

def concatenate_csv_files(file, item_input):
    """
    Reads and concatenates a list of CSV files located in a directory specified in a YAML configuration.

    Args:
        file (str): Path to the YAML config file.
        item_input (str): Item identifier specifying the directory containing CSV files.

    Returns:
        pandas.DataFrame: DataFrame containing the concatenated data from all CSV files.

    Raises:
        FileNotFoundError: If the specified directory containing CSV files does not exist.
        ValueError: If the YAML config file does not contain valid path information.
        IOError: If an error occurs while reading or concatenating the CSV files.
    """

    # get the path where the files to concatenate are stored using the get_path_item() function 
    dir_path= get_path_item(file, item_input)

    # get the value from YAML file
    yaml_value= get_yml_item_value(file, item_input)

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
    """
    Checks if all column names declared in the configuration exist in the DataFrame.

    Args:
        list_columns (list): List of column names declared in the configuration.
        data_frame_name (pandas.DataFrame): DataFrame to check for column existence.

    Returns:
        set or list: Set of column names that do not exist in the DataFrame if any, otherwise [0].

    """ 
    
    # returns the a set of the columns that does not exist
    if not set(list_columns).issubset(set(data_frame_name.columns)):
    
        return set(list_columns).difference(data_frame_name.columns)
    
    return [0]


def create_table_sql_statement(cursor, conn, sql_statement):
    """
    Executes a list of SQL statements.

    Args:
        cursor: Database cursor object.
        conn: Database connection object.
        sql_statement (list): List of SQL statements to execute.

    Returns:
        None

    Raises:
        psycopg2.Error: If an error occurs while executing the SQL statements.
    """
    for statement in sql_statement:
        
        try:
            
            cursor.execute(statement)
            
        except psycopg2.Error as e: 
            
            print("Error: Issue executing SQL statement")
            
            print(e)
            
        conn.commit()
        
        
def matching_elements_two_lists(first_list, second_list):
    """
    Compares two lists and returns elements that exist in both.

    Args:
        first_list (list): First list for comparison.
        second_list (list): Second list for comparison.

    Returns:
        list: Elements that exist in both lists.
    """

    # compare the template colums vs subset columns from config file and return NOT matches
    not_matching_elements = list(set(first_list).difference(second_list))

    if len(not_matching_elements) > 0:

        print('ATTENTION! The following columns do not exist in the data frame:', not_matching_elements)

    # Remove 'columns' form the list that does no exist in the template
    return [element for element in first_list if element not in not_matching_elements]

        
def iter_all_strings():    
    """
    Generates a continuous alphabetic list.

    Yields:
        str: Alphabetic strings in ascending order.
    """

    for size in itertools.count(1):
        
        for s in itertools.product(ascii_uppercase, repeat=size):
            
            yield "".join(s)


def list_characters(number):
    """
    Generates a list of alphabetic characters based on the number of rows to be assigned.

    Args:
        number (int): Number of rows to generate alphabetic characters for.

    Returns:
        list: List of alphabetic characters.
    """

    count = 1
    
    alpha_list = []

    for s in iter_all_strings():


        alpha_list.append(s)

        if count == number:

            break

        count += 1
        
    return alpha_list