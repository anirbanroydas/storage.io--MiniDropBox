import MySQLdb as mdb



def connect(host,user,passwd,dbname):
	""" Connect to a Database """
	con = mdb.connect(host,user,passwd,dbname)
	cur = db.cursor()
	return con,cur


def close(con):
	""" Close the database connection of database, db """
	con.close()


def query(cur,query):
	""" Execute the database query using the cursor """

	cur.execute(query)



