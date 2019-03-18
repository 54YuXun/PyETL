from Module.Developer import *
from Module.E_Read_CSV import *

def write_csv_file(ScheduleID,source_name,log_args,cmd,target_filepath,startDT,dataframe):
    
    Latest_csvpath = get_Latest_csvpath(target_filepath)
    msg = '(WC)Your file path: {}'.format(target_filepath) # CSV路徑
    debug(msg,1)
    
    try:
        ## 寫入 CSV
        dataframe.to_csv(target_filepath, index=False, encoding="utf8") 
        ## 寫入 "[Latest]CSV"
        dataframe.to_csv(Latest_csvpath, index=False, encoding="utf8") 

        ## 讀 CSV 計算長度
        csv_size = str(len(read_csv_file(ScheduleID,log_args,cmd,target_filepath,startDT))) 
        msg = '(WC)Row Count: {}'.format(csv_size)
        debug(msg,1)
        sp_to_schedule(ScheduleID,cmd,source_name,log_args,startDT,csv_size,1,'')
        return csv_size       
    except Exception as e:
        msg = '(WC)Create CSV Fail.' + str(e)
        debug(msg,1)
        sp_to_schedule(ScheduleID,cmd,source_name,log_args,startDT,0,0,msg)
        sys.exit(1)
        return -1
