from Module.Developer import *
## 忽略全部警告(目前只有datetime 格式寫入資料庫 date格式 自動截斷的警告)
import warnings 
warnings.filterwarnings("ignore")

## 寫入MySQL
def write_mysql(ScheduleID,log_args,file_parms,tablename,cmd,source_path,startDT,dataframe,database=None):
    
    if database is None:
        database = r'etl_utf8'

    source_name = source_path.split('\\')[-1]
    ## 呼叫 check() 避免 SQL Injection
    check_sql_injection(ScheduleID,log_args,cmd,source_path,startDT)

    db_tbn = "{}.{}".format(database,tablename)
    sp_query = get_sp_parms(db_tbn,file_parms)

    msg = "(WM)Call Stored Procedure: {}".format(sp_query)
    debug(msg,1)
    
    try :
        ## spC1:執行SP前資料筆數
        spC1 = get_table_count(db_tbn)  
        engine(database)[0].execute(sp_query)       
        ## spC2:執行SP後資料筆數    
        spC2 = get_table_count(db_tbn)  
    except Exception as e:
        msg = "(WM)Call Stored Procedure Error. {} ".format(str(e))
        debug(msg,1)
        sp_to_schedule(ScheduleID,cmd,source_name,log_args,startDT,0,0,msg)
        sys.exit(1)

    ## 計算執行SP前後影響筆數
    subcount = str(spC1 - spC2)
    msg_sp = "(WM)Stored Procedure deleted {} records.".format(subcount)
    debug(msg_sp,1)

    past = get_table_count(db_tbn)

    try:
        ## 寫入MySQL
        debug('(WM)Write to SQL...',1)
        dataframe.to_sql(tablename,engine(database)[0],if_exists="append",index=False) 
        ## now:寫入MySQL後資料筆數。
        now = get_table_count(db_tbn)   
        count = str(now - past)
        sp_to_schedule(ScheduleID,cmd,source_name,log_args,startDT,count,1,msg_sp)
        debug("(WM)Row count: {}".format(count),1)
        return count
    except Exception as e:
        ## error_count:寫入MySQL失敗後資料筆數。
        now = get_table_count(db_tbn)   
        error_count = str(now - past) 
        msg = '(WM)Csv To MySQL Fail. {}'.format(str(e))
        debug(msg,1)
        sp_to_schedule(ScheduleID,cmd,source_name,log_args,startDT,error_count,0,msg)
        return -1