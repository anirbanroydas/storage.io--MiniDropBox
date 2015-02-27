class recur:

	def __init__(self,path):
		self.__path = path


	def process(self):
		dname = os.path.split(self.__path)[1]
		dpath= os.path.split(self.__path)[0]

		for row in rows:
                    path2,filename2 = row[0] , row [1]
                    l = len(self.__srcpath)
                    
                    #fpath = os.path.join(path2[:l-1],destdname)
                    #print " fpath : ", fpath
                    #l2 = len(destdname)
                    finalpath = os.path.join(self.__destpath,path2[l:])
                    print "finalpath : ", finalpath
                    q = "UPDATE filesystem set path = '%s' where uID = %d and deviceID = %d and path = '%s' and filename = '%s' " % (finalpath,uId,deviceId,path2,filename2)
                    cur.execute(q)