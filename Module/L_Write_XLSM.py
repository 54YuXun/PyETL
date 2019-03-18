import openpyxl
from Module.Developer import *

def write_xlsm(ScheduleID,source_path,log_args,xlsm_path,sheet_name,cmd,target_filepath,startDT,dataframe):
    ## 取來源csv檔案名稱
    source_name = source_path.split('\\')[-1]

    try:
        ## 載入xlsm
        wb = openpyxl.load_workbook(xlsm_path, keep_vba=True)
    except Exception as e:
        msg = '(WX)Load xlsm fail.' + str(e)
        debug(msg,1)
        sp_to_schedule(ScheduleID,cmd,source_name,log_args,startDT,0,0,msg)
        sys.exit(1)
        return -1

    try:    
        ## 選取指定sheet
        sheet = wb.get_sheet_by_name(sheet_name)
    except Exception as e:
        msg = '(WX)sheet name is wrong.' + str(e)
        debug(msg,1)
        sp_to_schedule(ScheduleID,cmd,source_name,log_args,startDT,0,0,msg)
        sys.exit(1)
        return -1
    ## 從[A,2]開始寫入資料
    print(dataframe.iloc[0][0])
    for i in range(0,dataframe.shape[0]):
        count = dataframe.shape[0] - i - 1
        print('剩' + str(count) + '筆', end="\r")
        for j in range(0,dataframe.shape[1]):
            ## row +2 =不讀欄位 col +1 =不讀流水
            cell = sheet.cell(row=i+2, column=j+1) # 寫入[A,2]
            cell.value = dataframe.iloc[i][j] # 讀取CSV[A,1]不含欄位名稱

    ## 儲存到指定路徑
    wb.save(target_filepath)
    try:
        ## 讀取寫入後的xlsm，回傳資料筆數
        df = pd.read_excel(target_filepath,sheet_name=sheet_name)
        excel_length = str(len(df))
        sp_to_schedule(ScheduleID,cmd,source_name,log_args,startDT,excel_length,1,'')
        return excel_length
    except Exception as e:
        msg = '(WX)read excel fail.' + str(e)
        debug(msg,1)
        sp_to_schedule(ScheduleID,cmd,source_name,log_args,startDT,0,0,msg)
        sys.exit(1)
        return -1
