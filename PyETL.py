from Module.Developer import *
from Module.E_Read_SQL import *
from Module.L_Tableau import *
from Module.L_Write_CSV import *
from Module.L_Write_XLSM import *
from Module.L_MySQL import *

def main():
    startDT = get_datetime()
    check_local_log()
    ## input:hours
    check_old_files(4*24)
    ## parse arguments
    d, m, s, sid, q, x = get_argument()
    if sid is None:
        cmd = m[0]
        source_name = s[0]
        ScheduleID = 0
        log_args = get_args_from_local(d, q, x)
    else: 
        ScheduleID = sid[0]
        get_parms_from_scheduleID(ScheduleID)
        cmd, source_name, log_args = get_parms_from_scheduleID(ScheduleID)
    
    # folder_path = get_source_folder(source_path)
    source_path = get_source_path(source_name, ScheduleID, cmd, log_args, startDT)
    ## CSV To Xlsm
    if cmd == 'CX':
        xlsm_name,sheet_name = get_excel_file_sheet(cmd, log_args)
        xlsm_path = get_source_path(xlsm_name, ScheduleID, cmd, log_args, startDT)
        target_filepath = get_xlsm_filepath(source_path,xlsm_name)
        dataframe = read_csv_file(ScheduleID,log_args,cmd,source_path,startDT)
        write_xlsm(ScheduleID, source_path, log_args, xlsm_path, sheet_name, cmd, target_filepath, startDT ,dataframe)

    ## CSV to MySQL
    elif cmd == 'CM':
        tablename = log_args.split()[0]
        database,tablename = check_across_database(tablename)
        dataframe = read_csv_file(ScheduleID,log_args,cmd,source_path,startDT)
        file_params = get_csv_parms(source_path)
        write_mysql(ScheduleID,log_args,file_params,tablename,cmd,source_path,startDT,dataframe,database)

    ## Orcale To MySQL
    elif cmd == 'OM':

        tablename = log_args.split()[-1]
        database,tablename = check_across_database(tablename)
        file_parms = log_args.split()[:-1]
        ## [1]陣列，給SP [2]字串，給query 
        sp_parms, replace_parms = split_parms(file_parms)
        dataframe = read_sql_query(ScheduleID,replace_parms,cmd,source_path,startDT)
        write_mysql(ScheduleID,log_args,sp_parms,tablename,cmd,source_path,startDT,dataframe,database)

    else:
        ## 20180829
        folder_path = get_source_folder(source_path)

        target_filepath = get_target_file_path(cmd, folder_path, log_args)
        ## Read sql query and get dataframe
        dataframe = read_sql_query(ScheduleID,log_args,cmd,source_path,startDT)
        ## To CSV
        if cmd == 'OC' or cmd == 'MC':
            write_csv_file(ScheduleID, source_name, log_args, cmd, target_filepath, startDT, dataframe)
        ## To Tableau
        if cmd == 'OT' or cmd == 'MT':
            write_tableau(ScheduleID, source_name, log_args, cmd, target_filepath, startDT, dataframe)
        ## To Xlsm
        if cmd == 'OX' or cmd == 'MX':
            xlsm_name,sheet_name = get_excel_file_sheet(cmd, log_args)
            xlsm_path = get_source_path(xlsm_name, ScheduleID, cmd, log_args, startDT)
            write_xlsm(ScheduleID, source_path, log_args, xlsm_path, sheet_name, cmd, target_filepath, startDT ,dataframe)

if __name__ == "__main__":
    main()