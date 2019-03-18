from Module.Developer import *

def read_csv_file(ScheduleID,log_args,cmd,source_path,startDT):
    msg = "(RC)CSV Path: {}".format(source_path)
    debug(msg,1)
    ## 取CSV檔名
    source_filename = source_path.split('\\')[-1]  
    ## 讀"UTF8"或"BIG5"
    change_file_encoding(source_path)
    try:
        ## 讀取Csv回傳Dataframe
        dataframe = pd.read_csv(source_path,encoding= "utf8") # Read CSV
        return dataframe
    except Exception as e:
        msg = '(RC){}'.format(str(e))
        debug(msg,1)
        sp_to_schedule(ScheduleID,cmd,source_filename,log_args,startDT,0,0,msg)


        
