import adodbapi

def try_connection():

	db_prov = 'SQLOLEDB' # ADO can use OLE
	db_serv = 'st-vone-db.c50hlo4hzhce.ap-northeast-1.rds.amazonaws.com,1433'
	db_user = 'Administrator'
	db_pass = 'i04npsys'
	db_name = 'VOneG3'

	conn_string = 'Provider=%s;Data Source=%s;Initial Catalog=%s;User ID=%s;Password=%s;' % ( db_prov, db_serv, db_name, db_user, db_pass )

	#adodbapi.adodbapi.verbose = True
	conn = adodbapi.connect( conn_string )

	#return type( conn )
	return 'Connected'

print( try_connection() )
