import MySQLdb as mdb



def dbConnect(host,user,passwd,dbname):
	""" Connect to a Database """
	con = mdb.connect(host,user,passwd,dbname)
	cur = db.cursor()
	return con,cur


def dbClose(con):
	""" Close the database connection of database, db """
	con.close()


def dbQuery(cur,query):
	""" Execute the database query using the cursor """

	cur.execute(query)



