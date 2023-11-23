import pyodbc as db
import pandas as pd


conn = db.connect('Driver={SQL Server};'
                  'Server=DESKTOP-K5JQ2MV;'
                  'Database=SPT_ODS;'
                  'Trusted_Connection=Yes;')

cursor = conn.cursor()


# Membuat DataFrame
data = {'Nama': ['Johno', 'Janda'], 'Usia': [30, 45]}
database = pd.read_sql_query('SELECT [Inv Date],Customer,[Inv No],[Inv Price Bef Disc] FROM ODS_SALES', conn)

transactions = database
database.columns

orders = database.groupby(['Inv No', 'Inv Date', 'Customer']).agg({'Inv Price Bef Disc': lambda x: x.sum()})
orders.head()

# dd = {'Nama':data['Nama']}
# dc = df['Nama']
# df['Nama'] = df['Nama'].replace(dc,dd)

df = pd.DataFrame(None)
df = pd.DataFrame(database)

# Menyimpan DataFrame ke file Excel
df.to_excel(r'D:\Python\Sampel.xlsx', index=False)
# print(orders)



