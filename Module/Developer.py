import argparse
import sys, csv
import os, os.path
import pandas as pd
from sqlalchemy import exc
from sqlalchemy.sql import text
from sqlalchemy import create_engine
from datetime import datetime, timedelta

###########################################

MysqlPrep_SP = "etl_utf8.SP_Etl_MySQL_PREP"

###########################################


## 解析CMD上的參數
def get_argument():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", dest="d", action="store", help="MySQL Database and TableName. Example: test.zoo ", nargs=1)
    parser.add_argument("-m", dest="m", action="store", help="Choose mode. Eample: OT/MT....", nargs=1, choices=('OT', 'MT','OC','MC','OX','MX','CM','CX','OM',"ID"))
    parser.add_argument("-s", dest="s", action="store", help="Source file name. Example: O_XXX.sql/M_XXX.sql/O_XXX[20180601,20180602].csv", nargs=1)
    parser.add_argument("-sid", dest="sid", action="store", help="MySQL etl_schedule scheduleID. Example: 123 ", nargs=1, type=int)
    parser.add_argument("-q", dest="q", action="store", help="Parameter which replace variable in query.")
    parser.add_argument("-x", dest="x", action="store", help="Xlsm format file name and sheet name.")

    d = parser.parse_args().d
    m = parser.parse_args().m
    s = parser.parse_args().s
    sid = parser.parse_args().sid
    q = parser.parse_args().q
    x = parser.parse_args().x
    return d, m, s, sid, q, x

## 取得系統現在時間
def get_datetime():
    time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return time

## input: ScheduleID  output: Command, sourcefile, argument
def get_parms_from_scheduleID(ScheduleID):
    ## read schedule with scheduleID     
    sql = """
        select  concat(s.Command,' ',s.SourceFile,' ',s.Argument)AS 'Full_command'
        from abc s
        where s.scheduleID = """ + str(ScheduleID) + """
        """
    dataframe = pd.read_sql(sql, con=engine()[0])

    try:
        sql = """
        select  concat(s.Command,' ',s.SourceFile,' ',s.Argument)AS 'Full_command'
        from abc s
        where s.scheduleID = """ + str(ScheduleID) + """
        """
        command = dataframe.iloc[0]['Full_command']
    except Exception as e:
        msg = '(Main) {}'.format(str(e)[:99]) 
        debug(msg,1)
        sys.exit(1)
    cmd = command.split()[0] # 取指令(DB)
    source_name = command.split()[1] # 取檔名(DB)
    log_args = "" # 取引數(DB)
    for i in command.split()[2:]:
        log_args += "{} ".format(i)
    return cmd, source_name, log_args

## input: source_name  output: source_path
def get_source_path(source_name, ScheduleID, cmd, log_args, startDT):
    file = {}
    ## get current work directory
    location = os.getcwd()
    ## scan all files which in ETL directory.
    for root, dirs, files in os.walk(".", topdown=False):
        if "File" in root:
            for name in files:
                path = location + os.path.join(root, name)[1:]   
                ## 存成dictionary --> "檔案名稱":"檔案路徑"         
                file[name] = path
    ## 判斷檔名是否存在            
    if file.get(source_name) is None :
        msg = '(Error): Source Name error.'
        debug(msg,1)
        sp_to_schedule(ScheduleID,cmd,source_name,log_args,startDT,0,0,msg)
        sys.exit(1)
    else :
        ## 檔案來源路徑
        filepath = file[source_name]
    return filepath

## input: source_name  output: source_folder_path
def get_source_folder(source_path):
    folder_path = source_path.split('.')[0]
    if os.path.exists(folder_path) == 0:
        os.makedirs(folder_path)
    return folder_path

## 專案資料夾的最後一個CSV檔案路徑
def get_Latest_csvpath(target_filepath):
    ## 中括號內取代成"Latest"
    path = target_filepath.replace(target_filepath.split("[")[-1].split("]")[0],"Latest") 
    # print(path)
    if os.path.isfile(path) == 1:
        os.remove(path)
    return path

## input: -d -q -x arguments output: parameter string
def get_args_from_local(d, q, x):
    parm = ""
    if q is not None:
        for i in q.split():
            parm += "{} ".format(i)
    if d is not None:
        parm = parm + "{}".format(d[0])
    if x is not None:
        for j in x.split():
            parm = parm + "{} ".format(j)
    return parm

## 生成目標檔案路徑
def get_target_file_path(cmd, folder_path, log_args): 
    cmd = cmd[-1]
    filename_parm = ""
    folder_name = folder_path.split('\\')[-1]
    cmd_dic = {'C':'csv','T':'tde','X':'xlsm'}
    ## MX/OX時，去除最後兩個(xlsm_name, sheetname)
    if cmd == 'MX' or cmd == 'OX':
        xlsm_name = log_args.split()[-2].split('.')[0]
        for i in log_args.split()[:-2]:
            filename_parm += "{},".format(i)
    else:
        xlsm_name = log_args.split()[-2].split('.')[0]
        for i in log_args.split():
            filename_parm += "{},".format(i)

    filename_parm = filename_parm[:-1]
    path = folder_path + '\\{}[{}].{}'.format(folder_name,filename_parm,cmd_dic[cmd])
    ## 若是xlsm檔案，以xlsm檔名命名
    if cmd == 'X':       
        path = get_xlsm_filepath(path,xlsm_name)
    if os.path.isfile(path) == 1:
        os.remove(path)
    return path

## 改xlsm檔名，以原始檔案命名    
def get_xlsm_filepath(source_path,xlsm_name):
    source_filetype = source_path.split('.')[-1]
    source_filename = source_path.split('\\')[-1].split('[')[0]
    modify_path = source_path.split('\\')[-1].replace(source_filetype,'xlsm').replace(source_filename,xlsm_name.split('.')[0])
    path = "{}".format(source_path.replace(source_path.split('\\')[-1],modify_path))
    return path

## 從參數字串取得xlsm_name與sheet_name
def get_excel_file_sheet(cmd, log_args):
    if cmd == 'OX' or cmd == 'MX':
        excel_argument = log_args.split()
        xlsm_name = excel_argument[-2]
        sheet_name = excel_argument[-1]
    elif cmd == 'CX':
        xlsm_argument = log_args.split()
        xlsm_name = xlsm_argument[0]
        sheet_name = xlsm_argument[1]
    return xlsm_name, sheet_name

## 從CSV檔案中括號內，取出前兩個參數
def get_csv_parms(source_path):
    source_parameter = source_path.split('[')[-1].split(']')[0].split(',')[:2]
    return source_parameter

## 輸入表單名稱，輸出表單資料筆數 
def get_table_count(tablename):
    sql1 = "select count(*) as 'total' from {}".format(tablename)
    old = pd.read_sql(sql1,con=engine()[0])
    row_count  = old.iloc[0]['total']
    return row_count

## 給db.table & parms，回傳Query String
def get_sp_parms(db_tbn,file_parms):
    tablename = db_tbn.split('.')[0]
    sp_value = "\'{}\'".format(tablename)
    
    for i in file_parms:
        sp_value = sp_value + ",\'{}\'".format(i)
    sp_value = sp_value.replace(tablename,db_tbn)

    ## 生成 Query String --> call sp (datafield,tablename,datefrom,dateto)
    # sql1 ='call etl_utf8.SP_Etl_MySQL_PREP ({});'.format(sp_value)
    sql1 ='call {} ({});'.format(MysqlPrep_SP,sp_value)
    return sql1

## 確認是否有Log資料夾與當月Log
def check_local_log():  
    ## get current work directory
    location = os.getcwd()
    ## log floder path
    local_log_folder = location + "\\Log"
    ## check folder: if not exists, create new
    if os.path.exists(local_log_folder) == 0:
        os.makedirs(local_log_folder)  
    ## get now datetime example: 2018_08
    local_log_filename = datetime.now().strftime('%Y_%m')
    ## setting filename
    local_log_path = local_log_folder + '\\PyETL({}).csv'.format(local_log_filename)
    ## check log file: if not exists, create new
    if os.path.isfile(local_log_path) == 0 :
        with open(local_log_path, "w", newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',')
            ## column name
            writer.writerow(['Time', 'Event'])  
            csvfile.close()
        return local_log_path
    else:
        return local_log_path

## 刪除超過4天的檔案
def check_old_files(hr):
    ######  Setting time  ######
    ago = datetime.now() - timedelta(hours=hr)
    ######  Setting time  ######

    ## get current work directory
    location = os.getcwd()
    ## scan all file where in ETL folder
    for root, dirs, files in os.walk(".", topdown=False):
        if "File" in root:
            for File_Name in files:
                ## get file type
                datatype = File_Name.split(".")[-1]
                ## choose "TDE" and "CSV"
                if datatype.upper() == "TDE" or datatype.upper() == "CSV":
                    ## Remove "."   
                    ## Example: ".\SQL\M_test\M_test[20180601,20180602,zoo].csv"
                    path = location + os.path.join(root, File_Name)[1:]   
                    ## get file create time         
                    File_Createtime = datetime.fromtimestamp(os.path.getctime(path))
                    ## find old files
                    if File_Createtime < ago:
                        string = "(Delete Old): File_Name:{} File_Createtime:{}".format(File_Name,File_Createtime.strftime('%Y-%m-%d %H:%M:%S'))
                        os.remove(path)
                        debug(string,1)

## Check SQL Injection 
def check_sql_injection(ScheduleID,log_args,cmd,source_path,startDT):
    source_name = source_path.split('\\')[-1]
    query_argument = log_args.split()
    escape_words = ['-','--',';','\'','"','#','/','@','&',' and ',' exec ',' insert ',' select ',' delete ',' update ','*','%',' master ',' truncate ',' drop ',' declare ']
    ## 雙迴圈比對兩個陣列
    for i in escape_words:
        for j in query_argument:
            if i in j:
                msg = '(SQL)Error: "{}" is invalid argument.'.format(j)
                debug(msg,1)
                sp_to_schedule(ScheduleID,cmd,source_name,log_args,startDT,0,0,msg)
                sys.exit(1)

## 判斷輸入的table是否有跨資料庫
def check_across_database(tablename):
    if '.' in tablename:
        database = tablename.split('.')[0]
        tablename = tablename.split('.')[-1]
    else:
        database = None
    return database, tablename

## 讀"UTF8"或"BIG5"
def change_file_encoding(source_path):
    try:
        # test = open(source_path, 'r+',encoding='utf8').read().encode('big5', 'ignore').decode('big5')
        test = open(source_path, 'r+',encoding='utf8').read()
    except:
        test = open(source_path, 'r+',encoding='big5').read()
    f = open(source_path,"w+",encoding='utf8')
    f.write(test)
    f.close()

## call SP_Etl_ScheduleFromPyEtl and write to Schedule
def sp_to_schedule(scheduleID,cmd,source_name,log_args,startDT,count,result,Notes):
    endDT = get_datetime()
    if result == 1:
        string = 'Y'
    else:
        string = 'N'
    Notes = Notes[:99].replace('\'',' ') # 訊息取100個字
    sql ='call etl_utf8.to_abc(\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\');'.format(scheduleID,cmd,source_name,log_args,startDT,endDT,count,string,Notes)
    try:
        debug(sql,0) # 要print sql時, 0 --> 1
        engine()[0].execute(text(sql).execution_options(autocommit=True)) # 執行Sql指令
        engine()[0].dispose() # 關閉sql連結
    except Exception as e:
        msg = "(SQL)Write To Log Error. {}".format(str(e))
        debug(msg,1)
        sys.exit(1)
        
## 建立要取代的參數的字典 ex: {("&p1" : "datefrom")("&p1" : "dateto")}。
def replace_dic(source_path,query_argument):
    ## 取檔名前面,判斷M或O Example: O_SelectTableInCreateTime --> "O"
    char = source_path.split('\\')[-1].split('_')[0]
    ## 根據O/M使用不同符號
    varible_dic = {'O':'&','M':'@'}
    ## 建立字典
    dic ={}
    for i in range(0,len(query_argument)):        
        dic["{}p{}".format(varible_dic[char],i+1)] = "{}".format(query_argument[i])
    msg = '(RS)Your paramter: {}'.format(str(dic))
    debug(msg,1)
    return dic

## Function 將原字串取代掉字典的內容。
def replace_query_from_dic(text, dic):
    for i, j in dic.items():
        text = text.replace(i, j)
    return text

## 連結資料庫
## 呼叫engine[0] = MySQL 
def engine(mysql_db=None):  

    if mysql_db is None:
        mysql_db = r'etl_utf8'

    ## connect to MySQL
    mysql_account = 
    mysql_password = 
    mysql_host = 
    mysql_db_name = mysql_db
    mysql_engine = create_engine('mysql+pymysql://{}:{}@{}:3306/{}?charset=utf8'.format(mysql_account,mysql_password,mysql_host,mysql_db_name), echo=False)

    ## connect to orcale
    orcale_account = 
    orcale_password = 
    orcale_servicename = 
    orcale_engine = create_engine('oracle+cx_oracle://{}:{}@{}'.format(orcale_account,orcale_password,orcale_servicename), echo=False)
    
    return mysql_engine, orcale_engine

## (OM):參數分離成給sql與給sp的
def split_parms(file_parms):
    ## 給SP的Parameter
    sp_parms = file_parms[:2] 
    ## 要取代Query的Parameter
    replace_parms = "" 
    for i in file_parms:
        replace_parms += "{} ".format(i)
    return sp_parms, replace_parms

## Write to log and Print message
def debug(Event,toggle):
    ## get local_log_path
    local_log_path = check_local_log()
    ## get datetime when write to log 
    time = '{}'.format(datetime.now().strftime('%m/%d %H:%M:%S'))
    message = '{}'.format(Event)

    with open(local_log_path, "a", newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',')
        ## write to log 
        writer.writerow([time,message])
        csvfile.close()
    ##  when toggle = 0, it will not print on powershell
    if toggle == 1:
        print(message)

