import gspread as GS
import pandas as PD
from gspread_dataframe import set_with_dataframe
import pyodbc as db

conn = db.connect('Driver={SQL Server};'
                  'Server=DESKTOP-K5JQ2MV;'
                  'Database=SPT_ODS;'
                  'Trusted_Connection=Yes;')

cursor = conn.cursor()

database = PD.read_sql_query('SELECT TOP 39998 * FROM ODS_SALES', conn)

transactions = database
database.columns

orders = database #.groupby(['Inv No', 'Inv Date', 'Customer']).agg({'Inv Price Bef Disc': lambda x: x.sum()})
orders.head()

df = PD.DataFrame(None)
df = PD.DataFrame(database)

data_db = df

GSHEET_NAME = "Sales Data from Python" 
TAB_NAME = "sales"
GC = GS.service_account(filename='D:\Python\Pharse data Sales to Gsheet\Gsheet_key.json')

# def get_data(GSHEET_NAME,TAB_NAME,GC):
#     sh = GC.open(GSHEET_NAME)
#     worksheet = sh.worksheet(TAB_NAME)
#     dataframe = PD.DataFrame(worksheet.get_all_records())
#     return dataframe

# data = get_data(GSHEET_NAME,TAB_NAME,GC).iloc[:]

def write_data(GSHEET_NAME,TAB_NAME,GC,data_db):
    sh = GC.open(GSHEET_NAME)
    worksheet = sh.worksheet(TAB_NAME)
    set_with_dataframe(worksheet,data_db)
    return

print('Running...')
write_data(GSHEET_NAME,TAB_NAME,GC,data_db)
print('successed Post data to Google Sheet')
    



