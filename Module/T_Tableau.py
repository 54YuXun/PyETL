from Module.Developer import *
import tableausdk.Extract as tde
from tableausdk.Server import ServerAPI, ServerConnection

def create(target_filepath,dataframe):
    column = []
    ## 宣告Dataframe(左)pandas資料格式 對應 到TDE的資料格式(右)。
    fieldMap = {
        'float64' : tde.Types.Type.DOUBLE,
        'float32' : tde.Types.Type.DOUBLE,
        'int64' :   tde.Types.Type.INTEGER,
        'int8' :    tde.Types.Type.INTEGER,
        'object':   tde.Types.Type.UNICODE_STRING,
        'bool' :    tde.Types.Type.DOUBLE,
        'datetime64[ns]' :  tde.Types.Type.DATETIME
    }

    ## 若TDE已存在，則刪除。
    if os.path.isfile(target_filepath):
        os.remove(target_filepath)

    tdefile = tde.Extract(target_filepath)
    schema = tde.TableDefinition()  # define the table definition

    colnames = dataframe.columns   # dataframe all column name
    coltypes = dataframe.dtypes    # dataframe all column's datatype

    ## 根據Dataframe建立Tde Schema
    for i in range(0, len(colnames)):   
        cname = colnames[i]
        ctype = fieldMap.get(str(coltypes[i]))
        schema.addColumn(cname, ctype)  # 加入一行欄位(名稱，資料格式)
        column.append(cname)

    ## 開啟TDE FILE寫入資料
    with tdefile as extract:
        table = extract.addTable("Extract", schema)
        for r in range(0, dataframe.shape[0]):
            row = tde.Row(schema)
            ## Loop 顯示剩餘筆數
            count = dataframe.shape[0] - r - 1
            print('剩' + str(count) + '筆', end="\r")
            ## 每筆資料依照格式寫入TDE內
            for c in range(0, len(coltypes)):            
                if str(coltypes[c]) == 'float64':
                    if dataframe.iloc[r,c] is None:
                        row.setNull(c)
                    else:
                        row.setDouble(c, dataframe.iloc[r,c])
                elif str(coltypes[c]) == 'float32':
                    row.setDouble(c, dataframe.iloc[r,c])
                elif str(coltypes[c]) == 'int64':
                    if dataframe.iloc[r,c] is None:
                        row.setNull(c)
                    else:
                        row.setInteger(c, dataframe.iloc[r,c]) 
                elif str(coltypes[c]) == 'int8':
                    row.setInteger(c, dataframe.iloc[r,c])
                elif str(coltypes[c]) == 'object':
                    if dataframe.iloc[r,c] is None:
                        row.setNull(c)
                    else:
                        row.setString(c, dataframe.iloc[r,c])
                elif str(coltypes[c]) == 'bool':
                    row.setBoolean(c, dataframe.iloc[r,c])
                elif str(coltypes[c]) == 'datetime64[ns]':
                    try:
                        dt = dataframe.iloc[r,c]
                        row.setDateTime(c, dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second, 0)    
                    except:
                        row.setNull(c)
                else:
                    row.setNull(c)        
            ## insert the row    
            table.insert(row)   
    ## close the tdefile
    tdefile.close() 

## Publish TDE file to Server   
def publish(target_filepath,tableau_tablename):
    print(target_filepath)
    ## Server account
    hostname = r""
    username = r""
    password = r""
    siteID = r""
    projectName = r""
    ## Tableau tablename
    datasourceName = "" + tableau_tablename    
    overwrite = True
    try:
        ## Create the Server Connection Object
        serverConnection = ServerConnection()

        ## Connect to the Server
        serverConnection.connect( hostname, username, password, siteID )

        ## Publish the Extract to the Server
        ## Server Code 500
        serverConnection.publishExtract( target_filepath, projectName, datasourceName, overwrite )

        ## Disconnect from the Server
        serverConnection.disconnect()

        ## Destroy the Server Connection Object
        serverConnection.close()
    except Exception as e:
        debug(str(e),1)
        sys.exit(1)
