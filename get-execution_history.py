from pathlib import Path

import datetime

from TM1py import TM1Service
import os


def get_tm1_data (): 

    #######
    ## Copy history to the one file:
    #######
    
    dir_path = "C:\\Projects\\Models\\Process_clear\\tm1serverlog_History"

    sourcelogfiles = os.listdir(dir_path)  ## Get all files in the directory
    # sourcelogfiles = ["tm1server.log","tm1server.log1", "tm1server.log2", "tm1server.log3", "tm1server.log4", "tm1server.log5", "tm1server.log9"]
    
    
    tm1serverlog = "C:\Projects\Models\Process_clear\Logs\\tm1server.log"
    with open(tm1serverlog, "w") as outfile:
        for filename in sourcelogfiles:
            with open(dir_path + "\\" + filename) as infile:
                contents = infile.read()
                outfile.write(contents)
                
    ### Read tm1server.log file
    with TM1Service(address="localhost", port=8090, ssl=True, user="admin", password="") as tm1:
        

        log_entries = tm1.server.get_message_log_entries(
            logger="TM1.Process",
            since=datetime.datetime (2021, 1, 1, 0, 0, 0),      # (yyyy, m, d, h, m, s)
            until=datetime.datetime (2022, 10, 28, 0, 0, 0)       # (yyyy, m, d, h, m, s)
        )

        all_processes_name = tm1.processes.get_all_names()

        print("Total records readed: ", len(log_entries))

        return log_entries, all_processes_name

def parse_history(log_entries):
    ### Parse and write the result
    dic_Logger = {} # - create the dictionary of Loggers
    for log_line in log_entries:
        # if Message has a Logger name between double quotes
        if len(log_line['Message'].split('"'))>1:
            Logger_name = log_line['Message'].split('"')[1].strip()          
            
            if Logger_name not in dic_Logger:                   # add a new Logger to collection
                last_execution_date   = log_line['TimeStamp']  
                number_of_lines       = 1       
                first_execution_date  = log_line['TimeStamp']       
                source_message        = log_line['Message'] 
            else:                                                # update existing Logger             
                [last_execution_date, number_of_lines, first_execution_date, source_message] = dic_Logger[Logger_name]
                number_of_lines += 1        
                if log_line['TimeStamp'] > last_execution_date :
                    last_execution_date = log_line['TimeStamp']
                    source_message = log_line['Message']
                elif log_line['TimeStamp'] < first_execution_date:
                    first_execution_date = log_line['TimeStamp']
                    source_message = log_line['Message']    

            dic_Logger[Logger_name] = [last_execution_date, number_of_lines, first_execution_date, source_message]
        # print errors 
        else:
            print("Can't find a name in message: ", log_line['Message']) 

    return dic_Logger

def print_result_to_file(result_file, dic_Logger, all_names_list):
    with open( result_file, 'w') as textfile:
        # print header
        textfile.write('%s||%s||%s||%s||%s\n' % ("Name", "Last Execution Date", "Number of execution", "First Execution Date", "Message" ))
        for Logger_name in all_names_list:
            if Logger_name not in dic_Logger:
                textfile.write('%s\n' % Logger_name)
            else:
                textfile.write('%s||%s||%s||%s||%s\n' % (Logger_name, dic_Logger[Logger_name][0], dic_Logger[Logger_name][1], dic_Logger[Logger_name][2], dic_Logger[Logger_name][3] ))
        for Logger_name in dic_Logger:
            if Logger_name not in all_names_list:
                textfile.write('%s||%s||%s||%s||%s\n' % (Logger_name, dic_Logger[Logger_name][0], dic_Logger[Logger_name][1], dic_Logger[Logger_name][2], dic_Logger[Logger_name][3] ))



if __name__ == "__main__":

    ## Output Folder
    result_folder  = "C:\\Projects\\Models\\Process_clear\\Script Output"
    result_file = result_folder +"\\History_of_Process.txt"
 
    
    ## Get TM1 data
    #       log_entries - tm1server.log records "TM1.Process"
    #       all_names_list - the list of all TI processes
    
    log_entries, all_names_list = get_tm1_data()
    
    ## Create dictionary of TI processes with execution history
    dic_Logger = parse_history(log_entries)

    # Print to the output file
    print_result_to_file(result_file, dic_Logger, all_names_list)    



