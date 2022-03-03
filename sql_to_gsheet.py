import os
import sys
import datetime
import pyodbc
import pandas as pd
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import re
import ast
from google.auth import jwt
from google.cloud import pubsub_v1
from google.oauth2 import service_account
import socket

# References
# df2gspread documentation - https://readthedocs.org/projects/df2gspread/downloads/pdf/latest/

#Get the Source IP Address
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(("8.8.8.8", 80))
machine_ip = s.getsockname()[0]
s.close()
start_time = datetime.datetime.now()

# use creds to create a client to interact with the Google Drive API
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('gsheet_creds.json', scope)
gc = gspread.authorize(creds)

def pub_message(destination='', path='', message=''):
    time_delta = (datetime.datetime.now()-start_time)
    insert_row = {
        'Program' : 'SQL to GSheet',
        'Destination' : destination,
        'Machine_IP' : machine_ip,
        'Path' : path,
        'Message' : message,
        'Timestamp' : datetime.datetime.now(),
        'Duration' : time_delta.total_seconds()
    }
    service_account_info = json.load(open("gcp-sa.json"))
    audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
    creds = jwt.Credentials.from_service_account_info(
        service_account_info, audience=audience
    )
    data = str(json.dumps(insert_row, sort_keys=True, indent=4, default=str)).encode()
    publisher = pubsub_v1.PublisherClient(credentials=creds)
    topic_name = 'projects/{project_id}/topics/{topic}'.format(
        project_id='your-gcp-project',
        topic='your-pubsub-topic',
    )
    entry = publisher.publish(topic_name, data)
    entry.result()

def get_worksheet(gc, gfile_id, wks_name, write_access=False, new_sheet_dimensions=(1000, 100)):
    """DOCS..."""
    global spsh
    spsh = gc.open_by_key(gfile_id)

    # if worksheet name is not provided , take first worksheet
    if wks_name is None:
        wks = spsh.sheet1
    # if worksheet name provided and exist in given spreadsheet
    else:
        try:
            wks = spsh.worksheet(wks_name)
        except:
            wks = spsh.add_worksheet(
                wks_name, *new_sheet_dimensions) if write_access == True else None
    return wks


def upload_df(df, gfile="/New Spreadsheet", wks_name=None,col_names=True, row_names=True, clean=True, credentials=None, start_cell = 'A1', df_size = False, new_sheet_dimensions = (1000,100)):
    # access credentials
    # auth for gspread

    try:
        gc.open_by_key(gfile).__repr__()
        gfile_id = gfile
    except Exception as e:
        error_message = ast.literal_eval(str(e))['message']
        print(error_message)
        if error_message == 'The caller does not have permission':
            print('Error: ' + settings_folder_path + ' Make sure your GSheet is shared with the client_email in the gsheet_creds.json file');
            pub_message(gfile_id, settings_folder_path, 'Error: Make sure your GSheet is shared with the client_email in the gsheet_creds.json file')
            return
        elif error_message =='Requested entity was not found.':
            print('Error: ' + settings_folder_path + ' Your GSheet ID is not valid');
            pub_message(gfile_id, settings_folder_path,'Error: Your GSheet ID is not valid')
            return
        else:
            print('Error: ' + settings_folder_path + ' ' +  error_message + str(datetime.datetime.now()) + "\n")
            pub_message(gfile_id, settings_folder_path, 'Error: ' + error_message)
            return

    # Tuple of rows, cols in the dataframe.
    # If user did not explicitly specify to resize sheet to dataframe size
    # then for new sheets set it to new_sheet_dimensions, which is by default 1000x100
    if df_size:
        new_sheet_dimensions = (len(df), len(df.columns))
    wks = get_worksheet(gc, gfile_id, wks_name, write_access=True,
        new_sheet_dimensions=new_sheet_dimensions)


    start_col = re.split(r'(\d+)',start_cell)[0].upper()
    start_row = re.split(r'(\d+)',start_cell)[1]
    start_row_int, start_col_int = gspread.utils.a1_to_rowcol(start_cell)

    # find last index and column name (A B ... Z AA AB ... AZ BA)
    num_rows = len(df.index) + 1 if col_names else len(df.index)
    last_idx_adjust = start_row_int - 1
    last_idx = num_rows + last_idx_adjust

    num_cols = len(df.columns) + 1 if row_names else len(df.columns)
    last_col_adjust = start_col_int - 1
    last_col_int = num_cols + last_col_adjust
    last_col = re.split(r'(\d+)',(gspread.utils.rowcol_to_a1(1, last_col_int)))[0].upper()

    # If user requested to resize sheet to fit dataframe, go ahead and
    # resize larger or smaller to better match new size of pandas dataframe.
    # Otherwise, leave it the same size unless the sheet needs to be expanded
    # to accomodate a larger dataframe.
    if df_size:
        wks.resize(rows=len(df.index) + col_names, cols=len(df.columns) + row_names)
    if len(df.index) + col_names + last_idx_adjust > wks.row_count:
        wks.add_rows(len(df.index) - wks.row_count + col_names + last_idx_adjust)
    if len(df.columns) + row_names + last_col_adjust  > wks.col_count:
        wks.add_cols(len(df.columns) - wks.col_count + row_names + last_col_adjust)

    # Define first cell for rows and columns
    first_col = re.split(r'(\d+)',(gspread.utils.rowcol_to_a1(1, start_col_int + 1)))[0].upper() if row_names else start_col
    first_row = str(start_row_int + 1) if col_names else start_row

    # Addition of col names
    if col_names:
        cell_list = wks.range('%s%s:%s%s' % (first_col, start_row, last_col, start_row))
        for idx, cell in enumerate(cell_list):
            cell.value = df.columns.astype(str)[idx]
        wks.update_cells(cell_list)

    # Addition of row names
    if row_names:
        cell_list = wks.range('%s%s:%s%d' % (
            start_col, first_row, start_col, last_idx))

        for idx, cell in enumerate(cell_list):
            cell.value = df.index.astype(str)[idx]
        wks.update_cells(cell_list)


    # convert df values to string
    df = df.applymap(str)
    # Addition of cell values
    cell_list = wks.range('%s%s:%s%d' % (
        first_col, first_row, last_col, last_idx))
    for j, idx in enumerate(df.index):
        for i, col in enumerate(df.columns.values):
            if not pd.isnull(df[col][idx]):
                cell_list[i + j * len(df.columns.values)].value = df[col][idx]
    clear_range = "'" + wks_name + "'!" + first_col +  first_row + ":" + last_col
    spsh.values_clear(clear_range)
    wks.update_cells(cell_list, value_input_option='USER_ENTERED')
    return wks

with open('log/log.txt', 'a') as log:

    def resource_path(relative_path):
        if hasattr(sys, '_MEIPASS'):
            return os.path.join(sys._MEIPASS, relative_path)
        return os.path.join(os.path.abspath("."), relative_path)

    current_dir = os.getcwd()

    def getServerDbName(shortString):
        lowerString = str(shortString).lower()
        serverDBs = []
        try:
            with open('db_map.json') as content:
                db_map = json.loads(content.read())
                for setting in db_map:
                    for server in setting:
                        if lowerString == server:
                            for db_setting in setting[server]:
                                db_server = db_setting[0]
                                db_name = db_setting[1]
                                serverDBs.append([db_server, db_name])
                            continue
            print(serverDBs)
        except Exception as e:
            print('Error Processing db_map.json file :' + e)
        return serverDBs

    #Connects to SQL Server, runs the query and returns a dataframe
    def SQLConnect(serverDBs, query):
        try:
            print('Connecting to SQL Server...')
            connectionString = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=%s;DATABASE=%s;TRUSTED_CONNECTION=yes;Integrated Security=SSPI' % (serverDBs[0], serverDBs[1])
            openConnection = pyodbc.connect(connectionString)
            print('Connection Successful')
            print('Running Query...')
            print('Reading Results into a DataFrame...')
            df = pd.read_sql(query, openConnection)
            return df
        except Exception as e:
            print('Connection to SQL Server Failed', str(e))
            return str(e)

    #This is the main function when program is run, it takes a directory path to your BQ settings file and query file(s)
    def main():
            print('Program Starting***')
            sys_args = sys.argv
            final_df = pd.DataFrame()
            global settings_folder_path
            settings_folder_path = sys_args[1]
            settings_dir = os.listdir(settings_folder_path)

            #Loops through each file in the given directory
            for file in settings_dir:
                full_file = settings_folder_path + "\\" + file
                #Takes the Big Query settings file and pulls the BQ project, table, and if exists setting
                if 'gsheet_info' in file:
                    gsheet_config_file = open(full_file, 'r')
                    gsheet_settings = gsheet_config_file.read().split("\n")
                    global gsheet_id
                    global gsheet_tab
                    gsheet_id = gsheet_settings[0]
                    gsheet_tab = gsheet_settings[1]
                    gsheet_start_cell = gsheet_settings[2]
                    gsheet_config_file.close()
                #Every other file is treated as a query file
                #Takes the name of the file and splits it by "-", the first section is a short name for the database used in the getServerDBName function
                else:
                    try:
                        sql_settings = file.split('-')
                        sql_db = sql_settings[0]
                        serverDBs = getServerDbName(sql_db)
                        sql_query_name = sql_settings[1]
                        query_file = open(full_file, 'r')
                        #Reads the query from the contents of the file
                        query = query_file.read()
                        query_file.close()

                        #Runs the query
                        for server in serverDBs:

                            results = SQLConnect(server, query)
                            if isinstance(results, pd.DataFrame):
                            #Adds the dataframe to the final dataframe
                                final_df = pd.concat([final_df, results])
                            else:
                                print("Error processing " + file + " " + results)
                                pub_message(bq_dest_table, settings_folder_path,"Error processing " + file + " Server: " + server + " " + results)
                    # If errors occurs it ignores the file and moves to the next one
                    except Exception as e:
                        print("Error processing " + file, e)
                        pub_message(gsheet_id, settings_folder_path, 'Error Processing ' + full_file + " " + e )
                        continue


            #Resets the dataframe index and fills the NA values with blanks
            final_df = final_df.reset_index(drop=True)
            final_df = final_df.fillna('')
            # Once it has the final dataframe, it checks to make sure the GSheet settings have been set, then pushes the final dataframe to the GSheet
            if gsheet_id != '' and gsheet_tab != '':
                try:
                    print('Pasting data in GSheet')
                    upload_df(final_df, gsheet_id, wks_name= gsheet_tab, credentials=creds, row_names=False, start_cell=gsheet_start_cell)
                    pub_message(gsheet_id, settings_folder_path, 'Success')
                except Exception as e:
                    print('Error: ' + str(e))
                    pub_message(gsheet_id, settings_folder_path, 'Error pushing data to GSheet ' + gsheet_id + ' ' + str(e))
            print('***Program Finished***')



    if __name__ == "__main__":
        main()
