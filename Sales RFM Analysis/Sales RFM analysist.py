import schedule 
import pyodbc
import sqlite3
import pandas as pd
import datetime as dt
from datetime import timedelta

conn = pyodbc.connect('Driver={SQL Server};'
                  'Server=B1HANA;'
                  'Database=SPT_ODS;'
                  'UID=sa;'
                  'PWD=password#01;'
                  'Trusted_Connection=No;')
cursor = conn.cursor()

df = pd.read_sql_query('SELECT [Inv Date],Customer,[Inv No],[Inv Price Bef Disc] FROM ODS_SALES', conn)

transactions = df
transactions.columns

orders = transactions.groupby(['Inv No', 'Inv Date', 'Customer']).agg({'Inv Price Bef Disc': lambda x: x.sum()}).reset_index()
orders.head()

NOW = orders['Inv Date'].max() + timedelta(days=1)
# NOW = max(orders['Inv Date']) + timedelta(days=1)
# NOW = str(NOW)
# NOW =  dt.datetime.strptime(NOW,"%Y-%m-%d %H:%M:%S")
# NOW = NOW.strftime("%Y-%m-%d %H:%M:%S")
period = 365

orders['DaysSinceOrder'] = orders['Inv Date'].apply(lambda x: (NOW - x).days)

aggr = {
    'DaysSinceOrder': lambda x: x.min(),  # the number of days since last order (Recency)
    'Inv Date': lambda x: len([d for d in x if d >= NOW - timedelta(days=period)]), # the total number of orders in the last period (Frequency)
}
rfm = orders.groupby('Customer').agg(aggr).reset_index()
rfm.rename(columns={'DaysSinceOrder': 'Recency', 'Inv Date': 'Frequency'}, inplace=True)
rfm.head()

rfm['Monetary'] = rfm['Customer'].apply(lambda x: orders[(orders['Customer'] == x) & \
                                                           (orders['Inv Date'] >= NOW - timedelta(days=period))]\
                                                           ['Inv Price Bef Disc'].sum())
rfm.head()

quintiles = rfm[['Recency', 'Frequency', 'Monetary']].quantile([.2, .4, .6, .8]).to_dict()

def r_score(x):
    if x <= quintiles['Recency'][.2]:
        return 5
    elif x <= quintiles['Recency'][.4]:
        return 4
    elif x <= quintiles['Recency'][.6]:
        return 3
    elif x <= quintiles['Recency'][.8]:
        return 2
    else:
        return 1

def fm_score(x, c):
    if x <= quintiles[c][.2]:
        return 1
    elif x <= quintiles[c][.4]:
        return 2
    elif x <= quintiles[c][.6]:
        return 3
    elif x <= quintiles[c][.8]:
        return 4
    else:
        return 5
    
rfm['R'] = rfm['Recency'].apply(lambda x: r_score(x))
rfm['F'] = rfm['Frequency'].apply(lambda x: fm_score(x, 'Frequency'))
rfm['M'] = rfm['Monetary'].apply(lambda x: fm_score(x, 'Monetary'))
rfm['RFM_Score'] = rfm['R'].map(str) + rfm['F'].map(str) + rfm['M'].map(str)
rfm.head()

segt_map = {
    r'[1-2][1-2]' : 'hibernating',
    r'[1-2][3-4]' : 'at risk',
    r'[1-2]5' : 'can\'t loose',
    r'3[1-2]' : 'about to sleep',
    r'33' : 'need attention',
    r'[3-4][4-5]' : 'loyal customers',
    r'41' : 'promising',
    r'51' : 'new customers',
    r'[4-5][2-3]' : 'potential loyalists',
    r'5[4-5]' : 'champions'
}

rfm['Segment'] = rfm['R'].map(str) + rfm['F'].map(str)
rfm['Segment'] = rfm['Segment'].replace(segt_map, regex=True)
rfm.head()

dataframe = rfm

# print(dataframe)
# def trunc_to_table(): 
#     for index, row in dataframe.iterrows():
#         cursor.execute("TRUNCATE TABLE ODS_RFM")
#         # cursor.execute("INSERT INTO ODS_RFM (Customer,Recency,Frequency,Monetary,R,F,M,RFM_Score,Segment) values(?,?,?,?,?,?,?,?,?)", row.Customer, row.Recency, row.Frequency, row.Monetary, row.R, row.F, row.M, row.RFM_Score, row.Segment)
#     conn.commit()
#     cursor.close()
#     conn.close()
# schedule.every(20).seconds.do(trunc_to_table)

        # cursor.execute("TRUNCATE TABLE ODS_RFM")

def trunc_table(): 
    
    trunc_value = cursor.execute("TRUNCATE TABLE ODS_RFM")
    print("success to truncate")
    conn.commit()
    return trunc_value
schedule.every(2).seconds.do(trunc_table)    

def insert_value():
    for index, row in dataframe.iterrows():
        insert_value = cursor.execute("INSERT INTO ODS_RFM (Customer,Recency,Frequency,Monetary,R,F,M,RFM_Score,Segment) values(?,?,?,?,?,?,?,?,?)", row.Customer, row.Recency, row.Frequency, row.Monetary, row.R, row.F, row.M, row.RFM_Score, row.Segment)
        conn.commit()
    cursor.close()
    print("success to insert")
    return insert_value
schedule.every(3).seconds.do(insert_value)
# while True: 
#   schedule.run_pending()

trunc_table()
insert_value()

# print(func(test_value))
# print(test_value)
# print(insert_value)
# print(test_total)
# print(insert_value())



# test_value = pd.read_sql_query("select Count(Distinct Customer) as NUM FROM ODS_RFM",conn)
# test_value.head
# test_total = test_value

# def func(test_value): 
#     if (test_value == 0).all :
#         cursor.execute("TRUNCATE TABLE ODS_RFM")
#         print("success to truncate")
#         conn.commit()
#         cursor.close()
#     elif (test_value != 0).all :
#         for index, row in dataframe.iterrows():
#             cursor.execute("INSERT INTO ODS_RFM (Customer,Recency,Frequency,Monetary,R,F,M,RFM_Score,Segment) values(?,?,?,?,?,?,?,?,?)", row.Customer, row.Recency, row.Frequency, row.Monetary, row.R, row.F, row.M, row.RFM_Score, row.Segment)
#         print("success to insert")
#         conn.commit()
#         cursor.close()
#     else :
#         print("out of list")
        




#def call_me(): 
#     print(insert_value())
# schedule.every(30).seconds.do(call_me)
# while True: 
#   schedule.run_pending()

#   print('\n')
# print(insert_to_table)


# print(rfm)
# print('\n')
# print(transactions)
# print('\n')
# print(orders)
# print('{:,} rows, {:,} columns'.format(transactions.shape[0], transactions.shape[1]))
# print('{:,} invoices don\'t have a customer id'.format(transactions[transactions.Customer.isnull()].shape[0]))
 

