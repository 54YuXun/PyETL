from Module.T_TDE import *
from Module.Developer import *

def write_tableau(ScheduleID,source_name,log_args,cmd,target_filepath,startDT,dataframe):
    ## get current work directory
    location = os.getcwd()
    
    ## 建立TDE資料夾
    TDE_folder = location + "\\Log\\TDE"

    ## 判斷不存在就建立
    if os.path.exists(TDE_folder) == 0:
        os.makedirs(TDE_folder) 

    ## 改動環境變數，執行 To Tableau 的 Log 存進資料夾
    os.environ['TAB_SDK_LOGDIR'] = TDE_folder
    os.environ['TAB_SDK_TMPDIR'] = TDE_folder

    tableau_tablename = target_filepath.split('\\')[-2]  # tablename = sql檔案名稱
    
    ## 從 Dataframe 內 轉換判定格式成字串的欄位 轉換成 datetime 形式
    tde_size = str(len(dataframe))
    colnames = dataframe.columns
    try:
        for col in colnames:
            if dataframe[col].dtypes == 'object':     
                try:
                    ## 只有timestamp及datetime的dtype判為datetime, 其餘時間須手動轉
                    dataframe[col] = pd.to_datetime(dataframe[col],format='%Y-%m-%d %H:%M:%S')
                except ValueError:
                    pass
    except Exception as e:
        msg = '(WT)Convert Datetime Error. {}'.format(str(e))
        debug(msg,1)
        sp_to_schedule(ScheduleID,cmd,source_name,log_args,startDT,0,0,msg)
        sys.exit(1)


    tde.ExtractAPI.initialize() 
    try:
        debug('(WT)Start to create tde file.',1)
        ## Dataframe to TDE 
        create(target_filepath,dataframe)   
    except Exception as e:  
        msg = '(WT)Create TDE Fail. {}'.format(str(e))
        debug(msg,1)
        sp_to_schedule(ScheduleID,cmd,source_name,log_args,startDT,0,0,msg)
        sys.exit(1)
    tde.ExtractAPI.cleanup() 

    ServerAPI.initialize()    
    try:
        ## Publish TDE file to Server
        publish(target_filepath,tableau_tablename)
        debug('(WT)Publish TDE file success.',1)
        sp_to_schedule(ScheduleID,cmd,source_name,log_args,startDT,tde_size,1,'')
    except Exception as e:   
        msg = '(WT)Publish TDE file. {}'.format(str(e))
        debug(msg,1)
        sp_to_schedule(ScheduleID,cmd,source_name,log_args,startDT,0,0,msg)
        sys.exit(1)
    ServerAPI.cleanup() 

    msg = '(WT)Row Count : {}'.format(tde_size)
    debug(msg,1)
    return tde_size