import pandas as pd
import os
import time
import psycopg2
import psycopg2.extras as extras

# import functions file
import sys
file = 'functions.py'
sys.path.insert(0,os.path.dirname(os.path.abspath(file)))
import functions


def compare_column_names(file, item_input):

    '''
    function that compares columns from multiple data frames and returns a list of columns from the first file and the columns
    from the other data frames that are not in the first one.
    '''

    # get the path where the files to concatenate are stored using the get_path_item() function
    dir_path = functions.get_path_item(file, item_input)

    number = 0

    # read each file
    for file in os.listdir(dir_path):

        # if current path is a file do the column checking
        if os.path.isfile(os.path.join(dir_path, file)):

            path = dir_path + file

            # read the first file
            if number == 0:
                dataFrame = pd.read_csv(path)

            # read the second file and compare columns with the columns of first dataframe
            if number == 1:

                newDataFrame = pd.read_csv(path)

                # create a list with the columns names that are different
                lst_difference = newDataFrame.columns.difference(dataFrame.columns).values.tolist()

            else:

                newDataFrame = pd.read_csv(path)

                # compare column names that are different
                difference = newDataFrame.columns.difference(dataFrame.columns).values.tolist()

                # add the column name to the ist if they does not exist
                for element in difference:
                    lst_difference.append(element) if element not in lst_difference else lst_difference

            number += 1

    print('Output explanation: \n ([List columns on first file], \n [List columns NOT on first file])')

    return dataFrame.columns.values.tolist(), lst_difference


def extract_and_join_files(config_et_file_name, item_FILES_TO_PROCESS, item_UPDATE_COLUMN_NAMES, item_ADDITIONAL_INFORMATION_FILES,
                       item_JOIN_FILES_COMMON_COLUMNS):
    '''
    Join files located in the specified location. If the ADDITIONAL_INFORMATION_FILES block has a key-value, the columns of the additional file are
    added using the common columns specified in the JOIN_FILES_COMMON_COLUMNS block
    '''

    # get and concatenate all the files that are going to be proccessed. Function concatenate_csv_files() is used
    data_frame_to_process = functions.concatenate_csv_files_updating_column_names(config_et_file_name, item_FILES_TO_PROCESS, item_UPDATE_COLUMN_NAMES)

    ### If there is additional information to add, join to the dataframe. If not only change the name of the data frame
    # ADDITIONAL_INFORMATION_FILES
    # JOIN_FILES_COMMON_COLUMNS

    additional_information_files = functions.get_yml_item_value(config_et_file_name, item_ADDITIONAL_INFORMATION_FILES)

    # get and concatenate all the Experiment info files that are going to be proccessed. Function concatenate_csv_files() is used
    experiment_info_files = functions.concatenate_csv_files(config_et_file_name, item_ADDITIONAL_INFORMATION_FILES)

    # if directory path was specified in the configution file and the folder contains the additional files, they are going to be 
    # joined to the data files
    if additional_information_files != None and experiment_info_files is not None:

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

        joined_dataFrame = pd.merge(data_frame_to_process, experiment_info_files, on = columns_join_lower, how = 'left') 

    else:

        joined_dataFrame = data_frame_to_process

    print(joined_dataFrame.info())
        
    return joined_dataFrame


def drop_not_used_columns(config_et_file_name, item_COLUMNS_TO_DROP, dataFrame):
    
    '''
    Drop unused columns
    COLUMNS_TO_DROP
    '''
    print('Columns dataframe: ', dataFrame.columns)

    # get the list of columns that are going to be used to join the dataframes
    columns_to_drop = functions.get_yml_item_value(config_et_file_name, item_COLUMNS_TO_DROP)


    # if there is any value in the config file to drop, run the following code to drop the listed columns
    if not isinstance(columns_to_drop, type(None)):

        columns_to_drop_lower = [s.lower().replace(' ', '') for s in columns_to_drop]
        print('Columns to drop', columns_to_drop_lower)
        # drop columns from the dataframe
        # for each column in dataframe

        # for each column un list
        for column_list in columns_to_drop_lower:

            # delete if column of dataframe exists in list of columns
            if column_list in dataFrame.columns:

                print('Column to drop: ', column_list)

                #del dataFrame[col_df]
                dataFrame = dataFrame.drop(columns=column_list)

        # drop columns from the dataframe 
        #dataFrame = dataFrame.drop(columns = columns_to_drop_lower)

    print(dataFrame.info())

    return dataFrame


def update_column_values(config_et_file_name, item_UPDATE_ROW_VALUES, dataFrame):
    
    '''
    Update values column by column 
    VALUES_TRANSFORM
    '''
    ### Update row values 

    # get dict values from config file
    values_update =  functions.get_yml_item_value(config_et_file_name, item_UPDATE_ROW_VALUES)

    if values_update != None:

        # get values and keys from each dictionary element
        for key in values_update:

            value = values_update[key]

            # review each column to update the values    
            for column in dataFrame.columns:

                #print(joined_dataFrame[column])
                #print(column)

                dataFrame[column].mask(dataFrame[column] == key, value, inplace=True)
                
    print(dataFrame.head())
        
    return dataFrame


def add_new_columns(config_et_file_name, item_NEW_COLUMNS, dataFrame): 
    
    '''
    Add new columns to the dataFrame 
    NEW_COLUMNS
    '''    
    # new columns to add 
    new_columns = functions.get_yml_item_value(config_et_file_name, item_NEW_COLUMNS)

    if new_columns != None:

        number = len(dataFrame.columns)  # it will be used to get the dataframe position of new columns

        for key_column in new_columns:

            dataFrame.insert(number, key_column.lower(), new_columns.get(key_column))

            number += 1
            
    print(dataFrame.info())
            
    return dataFrame


def update_column_names(config_et_file_name, item_UPDATE_COLUMN_NAMES, dataFrame):
    
    '''
    Update column names
    UPDATE_COLUMN_NAMES
    '''    
    # new columns to add 
    update_column_names = functions.get_yml_item_value(config_et_file_name, item_UPDATE_COLUMN_NAMES)

    if update_column_names != None:

        for key, value in update_column_names.items():

            dataFrame.rename({key.lower() : value.lower()}, axis=1, inplace=True)

    # in case there are columns sharing the same name, merge them 
    
    #define function to merge columns with same names together
    def same_merge(x): return ','.join(x[x.notnull()].astype(str))
    
    #define new DataFrame that merges columns with same names together
    dataFrame = dataFrame.groupby(level=0, axis=1).apply(lambda x: x.apply(same_merge, axis=1))

    print(dataFrame.info())
            
    return dataFrame


def update_primary_key_values(config_et_file_name, item_UPDATE_PRIMARY_KEY_VALUES, item_PRIMARY_KEY_COLUMN, dataFrame):
    
    '''
    Update parts of the ID string values
    UPDATE_ID_VALUES
    '''
    # primary key values 
    primary_key_values = functions.get_yml_item_value(config_et_file_name, item_UPDATE_PRIMARY_KEY_VALUES)
    
    # get primary key
    primary_key = functions.get_yml_item_value(config_et_file_name, item_PRIMARY_KEY_COLUMN)[0].lower()

    if primary_key_values != None:

        for key, value in primary_key_values.items():
            
            dataFrame[primary_key] = dataFrame[primary_key].str.replace(key, value)
            
    print(dataFrame.head())
    
    return dataFrame


def export_dataframe(config_et_file_name, item_OUTPUT_FILE_NAME, dataFrame):   
    
    '''
    Export the new file created
    UTPUT_FILE_NAME
    '''
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
    Creates the database connection using the information provided in the DATABASE_CREDENTIALS item from the config_load.yml file. 
    '''
    # read information from config file
    database_credentials = functions.get_yml_item_value(config_load_file_name, item_DATABASE_CREDENTIALS)

    # num to check if it is the firsts value of a key
    num = len(database_credentials)

    # get values of conection database into a string
    for key in database_credentials:

        if num == len(database_credentials):

            DB_CONNECTION_STRING = key.lower() + '=%s ' % database_credentials[key] 

        else:

            DB_CONNECTION_STRING = DB_CONNECTION_STRING + key.lower() + '=%s ' % database_credentials[key] 

        num -= 1

    # delete last empty element from connection string
    DB_CONNECTION_STRING = DB_CONNECTION_STRING.rstrip(DB_CONNECTION_STRING[-1])

    ### Open database conection
    try: 
        connection = psycopg2.connect(DB_CONNECTION_STRING)

    except psycopg2.Error as e: 

        print("Error: Could not make connection to the Postgres database")
        print(e)

    try: 
        cursor = connection.cursor()

    except psycopg2.Error as e: 
        
        print("Error: Could not get cursor")
        print(e)

    connection.set_session(autocommit=True)
    
    return connection, cursor
    

def execute_sql_statement(cursor, connection, sql_statement):
    
    '''
    Function to create table in a database 
    '''
    
    for query in sql_statement:
        
        try:
            
            cursor.execute(query)
            
        except psycopg2.Error as e: 
            
            print("Error: issue executing sql statement")
            print (e)
            
        connection.commit()


def sql_statement_create_table_if_not_exists(config_load_file_name, item_NEW_TABLE_COLUMNS, item_TABLE_NAME):
    
    '''
    Create new table in the database (NEW_TABLE_COLUMNS)
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
    Prints the list created by get_columns.
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
    adds a new column when the table already exists (NEW_TABLE_COLUMNS)
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
    describe the structure of a table 
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
    insert values from a pandas dataframe into a postgresql table
    '''
    # get the table name
    table = functions.get_yml_item_value(config_load_file_name, item_TABLE_NAME)[0].lower()
    
    # get primary key
    primary_key = functions.get_yml_item_value(config_load_file_name, item_PRIMARY_KEY_COLUMN)[0].lower()

    # get file to upload using the csv_file_to_df() function
    dataframe = functions.csv_file_to_df(config_load_file_name, item_FILE_TO_UPLOAD)
  
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
        
    except (Exception, psycopg2.DatabaseError) as error:
        
        connection.rollback()
        
        return print("Error: %s" % error)
    
    print("Data has been inserted successfully!")