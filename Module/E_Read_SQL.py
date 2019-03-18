from Module.Developer import *

## 開啟檔案讀取Query，回傳Dataframe。
def read_sql_query(ScheduleID,log_args,cmd,source_path,startDT):
    source_name = source_path.split('\\')[-1]
    ## 呼叫check()避免SQL Injection
    check_sql_injection(ScheduleID,log_args,cmd,source_path,startDT)

    ## 字串處理:引數(Local)
    if cmd[-1] == 'X':
        query_argument = log_args.split()[:-2]
    else :
        query_argument = log_args.split()
    ## 讀"UTF8"或"BIG5"
    change_file_encoding(source_path)

    ## 開啟query檔案
    with open (source_path, 'r+', encoding="utf8") as f :
        ## 開啟 Sql 檔案讀內部的 Query。
        query = f.read()    
        ## 取代後的 Query。
        query = replace_query_from_dic(query, replace_dic(source_path,query_argument)) 
        msg_query = '(RS)Your query:\n{}'.format(query)

        ## 讀 Oracle 時,須改環境變數成 "UTF-8" 編碼。
        if cmd[:1] == 'O':
            os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.AL32UTF8'  
            con = engine()[1]    
        elif cmd[:1] == 'M':
            con = engine()[0]

        try:
            ## read query
            dataframe = pd.read_sql(query, con=con) 
            f.close()
            return dataframe
        except exc.SQLAlchemyError as e:
            f.close()
            
            ## SQL錯誤訊息取77個字元 and 取代掉特殊字元
            error_msg = str(e)[:77].replace('\'','').replace('"','')    
            msg = '(RS) {}'.format(error_msg)
            debug(msg,1)
            sp_to_schedule(ScheduleID,cmd,source_name,log_args,startDT,0,0,msg)
            sys.exit(1)
