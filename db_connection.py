import pyodbc

server = 'localhost,11433'
database = 'VOneG3'
username = 'Administrator'
password = 'i04npsys'
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';trusted_connection=yes;UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
print ('Inserting a new row into table')


# import pyodbc
# conn = pyodbc.connect("DRIVER={{SQL Server}};SERVER={0}; database={1}; \
#        trusted_connection=yes;UID={2};PWD={3}".format(server,MSQLDatabase,username,password))
# cursor = conn.cursor()
# 
# import pydobc
#
# def lambda_handler(event, context):
#     cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=servername.account.region.rds.amazonaws.com,port;DATABASE=database;UID=user;PWD=password')
