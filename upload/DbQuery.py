
def dbConnect(host,user,passwd,dbname):
	""" Connect to a Database """
	db = MySQLdb.connect(host,user,passwd,dbname)
	cursor = db.cursor()
	return db,cursor


def dbClose(db):
	""" Close the database connection of database, db """
	db.close()


def dbQuery(cursor,query):
	""" Execute the database query using the cursor """

	cursor.execute(query)



