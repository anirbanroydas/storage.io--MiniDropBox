""" FileSplitter -
Split a file into multiple small files


"""

import os, sys
import hashlib
import string
import MySQLdb as mdb


uID = 123
deviceID = 1231


class FileSplitterException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return str(self.value)

def usage():
    return """\nUsage: FileSplitter.py -i <inputfile> -n <chunksize> [option]\n
    Options:\n
    -s, --split  Split file into chunks
    -j, --join   Join chunks back to file.
    """



class FileSplitter:
    """ File splitter class """

    

    def __init__(self):

        print "\n initializing FileSplitter object "
        # cache filename
        self.__filename = ''
        # number of equal sized chunks
        self.__numchunks = 0
        # Size of each chunk
        self.__chunksize = 0
        # Optional postfix string for the chunk filename
        self.__postfix = ''
        # path name
        self.__path = ''
        # extreme bname i.e filename exclusive of path
        self.__bname = ''
        # Action
        self.__action = 0 # split
        self.__fmultiplier = 1048576
        self.__commitQlist = []
        self.__needblocks = []
        self.__blockdict = {}
        print "\n __fmultiplier : ", self.__fmultiplier
        print "\n __commitQlist : ",self.__commitQlist
        print "\n __blockdict : ", self.__blockdict

    def parseOptions(self, args):
        """ when used as a script then partse the argv list and set the atrributes"""

        import getopt

        try:
            optlist, arglist = getopt.getopt(args, 'sji:n:', ["split=", "join="])
        except getopt.GetoptError, e:
            print e
            return None

        for option, value in optlist:
            if option.lower() in ('-i', ):
                self.__filename = value
            elif option.lower() in ('-n', ):
                self.__chunksize = int(value)*self.__fmultiplier
            elif option.lower() in ('-s', '--split'):
                self.__action = 0 # split
            elif option.lower() in ('-j', '--join'):
                self.__action = 1 # combine

        if not self.__filename:
            sys.exit("Error: filename not given")


    def parseValues(self,filename,chunksize,action) :
        """ when used as a module, then parse the given parameters as arguments and set the atrributes"""

        self.__filename = filename
        self.__chunksize = int(chunksize)*self.__fmultiplier
        self.__action = int(action)
        
    def do_work(self):
        if self.__action==0:
            self.split()
        elif self.__action==1:
            self.combine()
        else:
            return None
        
    def split(self):
        """ Split the file and save chunks
        to separate files """

        
        try:
            f = open(self.__filename, 'rb')
        except (OSError, IOError), e:
            raise FileSplitterException, str(e)


        self.__bname = (os.path.split(self.__filename))[1]
        bname = self.__bname
        print '\n bname : ',bname
        self.__path = (os.path.split(self.__filename))[0]
        print '\n path : ',self.__path

        # Get the file size
        fsize = os.path.getsize(self.__filename)
        print "\nfile size : ",fsize
        print
        # Get no. of chunks
        if fsize%self.__chunksize==0:
            self.__numchunks = int(float(fsize)/float(self.__chunksize))
        else:
            self.__numchunks = int(float(fsize)/float(self.__chunksize)) + 1


        print self.__numchunks
        chunksz = self.__chunksize
        total_bytes = 0

        # First find the file hash and send to m-server for availability

        h = hashlib.sha256()

        # For bigger files, thus break the full filehashig procedure into smaller chunks

        for x in range(self.__numchunks):

            # if reading the last section, calculate correct
            # chunk size.

            if x == self.__numchunks - 1:
                chunksz = fsize - total_bytes

            data = f.read(chunksz)
            h.update(data)
            total_bytes += len(data)

        filehash = h.hexdigest()
        print
        print 'filehash : ',filehash

        # Send filehash to m-server
        print "\nSending filehash to m-server.......\n"

        success = self.send_filehash_mserver(filehash)
        print ' success :  ',success

        if success :
            print "\nfilehash exists, thus file already present \n"
            print 'file : ',self.__bname,' UPLOADED \n'
            f.close()
            return


        # Split file if no file level match

        f.seek(0)   # since f.read() has put the cursor of the file to the end of the file
        chunksz = self.__chunksize 
        total_bytes = 0
        hashlist = []
        #blockdict = {}
        hashlist.append(filehash)

        print 'Splitting file', self.__filename
        print 'Chunk Size', self.__chunksize, '\n'

        chunkdirectory = "/home/cdot/Documents/Storage.0/chunkstore/"
        print 'chunk file directory path name : ',chunkdirectory,'\n'

        for x in range(self.__numchunks):

            chunkfilename = bname + '-' + str(x+1) + self.__postfix

            # if reading the last section, calculate the correct chunk size
            if x == self.__numchunks - 1:
                chunksz = fsize - total_bytes

         
            # chunk the file and calculate chunk hashes simultaneously
            try:
                print 'Writing file',chunkfilename
                data = f.read(chunksz)
                total_bytes += len(data)
                chunkf = file(os.path.join(chunkdirectory,chunkfilename), 'wb')
                chunkf.write(data)
                chunkf.close()
                ch = hashlib.sha256()
                ch.update(data)
                chunkhash = ch.hexdigest()
                print
                print 'chunkhash ',str(x+1),' : ',chunkhash
                hashlist.append(chunkhash)
                self.__blockdict[chunkhash]=chunkfilename
                print 
                print 'chunk size : ', len(data)
                print 'chunk ',str(x+1),' created'
            except (OSError, IOError), e:
                print e
                continue
            except EOFError, e:
                print e
                break

        print 'Done.'

        print '\nhash list : \n'
        print hashlist
        print '\n\n block Dictionary : \n'
        print self.__blockdict
        f.close()

        #send chunk hash list to meta server for checking which chunks already present 
        print "\nsending chunk hash list to meta server for chunk availability "

        self.send_chunkhashlist_mserver(hashlist)

        # store the blocks needed by meta server to the storage server using the needblocks list
        print "\n Stoe the needblocks \n"

        ack = self.storeblocks()
        print '\n blocks stored \n'

        # Commit the result to metaserver using the commitQlist

        if ack==1:
            print "\n will Commit the Query .....\n"
            self.commitQuery()
            print "\n commited the Queries \n"
            print " erasing the query logs and needblocks logs......\n"
            self.__commitQlist = []
            self.__needblocks = []
            self.__blockdict = {}
        else:
            print "\n Error uploading data "







    def send_filehash_mserver(self,filehash):
        """ send the filehash to the meta server for checking availability"""
        #self.filehash = filehash
        fh = filehash
        print 'filehash :  ',fh
        print "\nconnecting to storage0 database.....\n"
        con = mdb.connect('localhost','storage0user','storage0roy','storage0')
        print "connection established .... \n"
        cur = con.cursor()
        q = "SELECT count from filehashT where filehash= '%s' " % (fh)
        print "query : ",q,'\n'
        cur.execute(q)

        #if match found update filehastT's content and chunkhasT's content and add the list to filesystem
        if cur.rowcount>0 :
            print " match found \n"

            commitQ = "UPDATE filehashT set count = count + %d where filehash = '%s' " % (1,fh)
            #self.commitQlist.append(commitQ)
            cur.execute(commitQ)
            commitQ = "UPDATE chunkhashT set count = count + %d where filehash = '%s' " % (1,fh)
            #self.commitQlist.append(commitQ)
            cur.execute(commitQ)

            # get user id, device id, path, filename, filehash, version , share id and add that row in filesystem

            uId, deviceId = self.getUserInfo()
            path,filename = self.getFileInfo()

            ## To check if a file with same name already exists in the folder path

            # checkQ = "SELECT count(path),count(filename) from filesystem where uID = %d  and deviceID = %d and path = '%s' and filename = '%s' " % (uId,deviceId,path,filename)
            # print " \ncheckQ : ",checkQ
            # cur.execute(checkQ)
            # row = cur.fetchall()
            # print "\n row : ",row

            # if row[0]>0 and row[1]>0 :
            #     print "\n A file with the same name already exisits, please rename the file \n"
            #     return
            # else:


            commitQ = "INSERT into filesystem (uID,deviceID,path,filename,filehash,version) values (%d,%d,'%s' ,'%s', '%s',1)" % (uId,deviceId,path,filename,fh)
            cur.execute(commitQ)
            con.commit()
            cur.close()
            con.close()

            return 1

        else:
            print " match not found \n"
            cur.close()
            con.close()
            commitQ = "INSERT into filehashT (filehash,count) values ('%s',1)" % (fh)
            print " CommitQ  :  ",commitQ, '\n'
            self.__commitQlist.append(commitQ)
            return 0



    def getUserInfo(self):

        return uID, deviceID


    def getFileInfo(self):

        return self.__path, self.__bname


    def send_chunkhashlist_mserver(self,hashlist):
        """ send the chunkhashlist to the meta server for checking availability of chunk's hashes """

        chl = hashlist[1:]
        print " \nchunk hash : ",chl
        #needblocks = []
        print '\n Connecting to meta server to Storage0 database......\n'
        con = mdb.connect('localhost','storage0user','storage0roy','storage0')
        print '\n connectio established.....\n'
        cur = con.cursor()
        for ch in chl:
            q = "SELECT count from chunkhashT where chunkhash= '%s' " % (ch)
            print '\nQuery : ',q,'\n'
            cur.execute(q)
            if cur.rowcount == 0:
                print "\n chunkhash not present \n"
                self.__needblocks.append(ch)
                #commitQ = "INSERT into chunkhashT (filehash,chunkhash,count) values ('%s','%s',1) " % (hashlist[0],ch,1)
                #self.commitQlist.append(commitQ)
            #else:
                #commitQ = "UPDATE chunkhashT set count = count + %d where chunkhash = '%s' " % (1,h)
                #commitQ = "INSERT into chunkhashT (filehash, chunkhash, count) values ( '%s', '%s', 1) " % (hashlist[0],)
                #self.commitQlist.append(commitQ)
            commitQ = "INSERT into chunkhashT (filehash, chunkhash, count) values ( '%s', '%s', 1) " % (hashlist[0],ch)
            print '\n commitQ : ',commitQ,'\n'
            self.__commitQlist.append(commitQ)

        uId, deviceId = self.getUserInfo()
        path,filename = self.getFileInfo()

        commitQ = "INSERT into filesystem (uID,deviceID,path,filename,filehash,version) values (%d,%d,'%s' ,'%s', '%s',1)" % (uId,deviceId,path,filename,hashlist[0])
        print '\n commitQ : ',commitQ,'\n'
        self.__commitQlist.append(commitQ)
        con.commit()
        cur.close()
        con.close()
        print '\n needed Blocks : ', self.__needblocks,'\n'
        print '\n commitQ list : ',self.__commitQlist,'\n'



    def storeblocks(self):
        """ Store the blocks in the Storage server """

        ack = 0
        for b in self.__needblocks:
            # store the block in Storage server
            # ack = store(b,self.blockdict[b])
            print "\n Storing Block - '%s' and chunk - '%s' \n" % (b,self.__blockdict[b]) 


        # return ack
        return 1


    def commitQuery(self):
        """ Commit the Query , i.e execute left over queries in the commitQlist """
        print "\n Connecting to Storage0 database .....\n"
        con = mdb.connect('localhost','storage0user','storage0roy','storage0')
        print "\n connection established \n"
        cur = con.cursor()

        for q in self.__commitQlist:
            print '\nquery : ',q,'\n'
            cur.execute(q)


        con.commit()
        cur.close()
        con.close()

        






    def sort_index(self, f1, f2):

        index1 = f1.rfind('-')
        index2 = f2.rfind('-')
        
        if index1 != -1 and index2 != -1:
            i1 = int(f1[index1:len(f1)])
            i2 = int(f2[index2:len(f2)])
            return i2 - i1
        
    def combine(self):
        """ Combine existing chunks to recreate the file.
        The chunks must be present in the cwd. The new file
        will be written to cwd. """

        import re
        
        print 'Creating file', self.__filename
        
        bname = (os.path.split(self.__filename))[1]
        bname2 = bname
        
        # bugfix: if file contains characters like +,.,[]
        # properly escape them, otherwise re will fail to match.
        for a, b in zip(['+', '.', '[', ']','$', '(', ')'],
                        ['\+','\.','\[','\]','\$', '\(', '\)']):
            bname2 = bname2.replace(a, b)
            
        chunkre = re.compile(bname2 + '-' + '[0-9]+')
        
        chunkfiles = []
        for f in os.listdir("."):
            print f
            if chunkre.match(f):
                chunkfiles.append(f)


        print 'Number of chunks', len(chunkfiles), '\n'
        chunkfiles.sort(self.sort_index)

        data=''
        for f in chunkfiles:

            try:
                print 'Appending chunk', os.path.join(".", f)
                data += open(f, 'rb').read()
            except (OSError, IOError, EOFError), e:
                print e
                continue

        try:
            f = open(bname, 'wb')
            f.write(data)
            f.close()
        except (OSError, IOError, EOFError), e:
            raise FileSplitterException, str(e)

        print 'Wrote file', bname




class FileModify :
    """ file modification class """

    def __init__(self,filepath,chunksize):

        self.__filepath = filepath
        #self.__chunksize = 0
        self.__fmultiplier = 1048576
        self.__chunksize = int(chunksize)*self.__fmultiplier
        #self.__action = 0


    def getUserInfo(self):

        return uID, deviceID


    def getFileInfo(self):

        self.__bname = (os.path.split(self.__filepath))[1]
        bname = self.__bname
        print '\n bname : ',bname
        self.__path = (os.path.split(self.__filepath))[0]
        print '\n path : ',self.__path

        return self.__path, self.__bname




    def calHash(self):
        """ Calculates the hash of the file"""


        try:
            f = open(self.__filepath, 'rb')
        except (OSError, IOError), e:
            raise FileSplitterException, str(e)


        fpath,fname = self.getFileInfo()

        # Get the file size
        fsize = os.path.getsize(self.__filepath)
        print "\nfile size : ",fsize
        print
        # Get no. of chunks
        if fsize%self.__chunksize==0:
            self.__numchunks = int(float(fsize)/float(self.__chunksize))
        else:
            self.__numchunks = int(float(fsize)/float(self.__chunksize)) + 1


        print self.__numchunks
        chunksz = self.__chunksize
        total_bytes = 0

        # First find the file hash and send to m-server for availability

        h = hashlib.sha256()

        # For bigger files, thus break the full filehashig procedure into smaller chunks

        for x in range(self.__numchunks):

            # if reading the last section, calculate correct
            # chunk size.

            if x == self.__numchunks - 1:
                chunksz = fsize - total_bytes

            data = f.read(chunksz)
            h.update(data)
            total_bytes += len(data)

        filehash = h.hexdigest()
        print
        print 'filehash : ',filehash

        # Send filehash to m-server
        print "\nSending filehash to m-server.......\n"

        success = self.send_filehash_mserver(filehash)
        print ' success :  ',success

        return success





    def send_filehash_mserver(self,filehash):
        """ send the filehash to the meta server for checking availability"""
        #self.filehash = filehash
        fh = filehash
        print 'filehash :  ',fh
        print "\nconnecting to storage0 database.....\n"
        con = mdb.connect('localhost','storage0user','storage0roy','storage0')
        print "connection established .... \n"
        cur = con.cursor()
        q = "SELECT count from filehashT where filehash= '%s' " % (fh)
        print "query : ",q,'\n'
        cur.execute(q)

        #if match found update filehastT's content and chunkhasT's content and add the list to filesystem
        if cur.rowcount>0 :
            print " match found \n"

            

            # delete the old filehash or the count of the old filehash from filehashT and counts of old chunkhashes corresponding to given old filehash from chunkhashT
            fdel = fileSplit.FileDelete(self.__filepath)
            fdel.del4mFilesystem()

            # add new entry into fileysytem, filehashT, chunkshashT

            commitQ = "UPDATE filehashT set count = count + %d where filehash = '%s' " % (1,fh)
            #self.commitQlist.append(commitQ)
            cur.execute(commitQ)
            commitQ = "UPDATE chunkhashT set count = count + %d where filehash = '%s' " % (1,fh)
            #self.commitQlist.append(commitQ)
            cur.execute(commitQ)

            # get user id, device id, path, filename, filehash, version , share id and add that row in filesystem

            uId, deviceId = self.getUserInfo()
            path,filename = self.getFileInfo()

            commitQ = "INSERT into filesystem (uID,deviceID,path,filename,filehash,version) values (%d,%d,'%s' ,'%s', '%s',1)" % (uId,deviceId,path,filename,fh)
            cur.execute(commitQ)

            


            con.commit()
            cur.close()
            con.close()

            return 1

        else:
            print " new filehash not found \n"
            cur.close()
            con.close()
           

            return 0








class FileDelete :
    """ File Deletion Class"""

    def __init__(self,filename):

        self.__filename = filename


    def getUserInfo(self):

        return uID, deviceID


    def getFileInfo(self):

        self.__bname = (os.path.split(self.__filename))[1]
        bname = self.__bname
        print '\n bname : ',bname
        self.__path = (os.path.split(self.__filename))[0]
        print '\n path : ',self.__path

        return self.__path, self.__bname



    def del4mFilesystem(self):
        """ Delete the entry from table Filesystem"""

        print "\n Connecting to Storage0 database .....\n"
        con = mdb.connect('localhost','storage0user','storage0roy','storage0')
        print "\n connection established \n"
        cur = con.cursor()

        uId, deviceId = self.getUserInfo()
        path,filename = self.getFileInfo()

        q = "SELECT filehash from filesystem where uID = %d and deviceID = %d and path = '%s' and filename = '%s' " % (uId,deviceId,path,filename)
        cur.execute(q)

        if cur.rowcount>0 :
            fh = cur.fetchone()[0]

            # Delete entry from filesystem table
            q = "DELETE from filesystem where uID = %d and deviceID = %d and path = '%s' and filename = '%s' " % (uId,deviceId,path,filename)
            cur.execute(q)

            q = "SELECT count from filehashT where filehash = '%s' " % (fh)
            cur.execute(q)

            # Delete entry from filehashT Table
            if cur.rowcount>0 :
                c = cur.fetchone()[0]
                if c>1:
                    q = "UPDATE filehashT set count = count - %d where filehash = '%s' " % (1,fh)
                    cur.execute(q)
                else :
                    q = "DELETE from filehashT where filehash = '%s' " % (fh)
                    cur.execute(q)
            else:
                print "\n Unknowing error : since no entry with given filehash present "


            #Delete entry from chunkhashT table
            q = "SELECT * from chunkhashT where filehash = '%s' " % (fh)
            cur.execute(q)

            rows = cur.fetchall()

            for row in rows:
                rfh,rch,rc = row[0],row[1],row[2]
                if rc>1:
                    q = "UPDATE chunkhashT set count = count - %d where filehash = '%s' and chunkhash = '%s' " % (1,rfh,rch)
                    cur.execute(q)
                else :
                    q = "DELETE from chunkhashT where filehash = '%s' and chunkhash = '%s' " % (rfh,rch)
                    cur.execute(q)

        else :
            print "\n Unfortunately no such file in such path exist "


        con.commit()
        cur.close()
        con.close()


class FileMove :
    """ File move/rename Class """

    def __init__(self,srcfilename,destfilename):

        self.__srcfilename = srcfilename
        self.__destfilename = destfilename

    
    def getUserInfo(self):

        return uID, deviceID


    def getFileInfo(self):

        self.__srcbname = (os.path.split(self.__srcfilename))[1]
        bname = self.__srcbname
        print '\n srcbname : ',bname
        self.__srcpath = (os.path.split(self.__srcfilename))[0]
        print '\n srcpath : ',self.__srcpath

        self.__destbname = (os.path.split(self.__destfilename))[1]
        bname = self.__destbname
        print '\n destbname : ',bname
        self.__destpath = (os.path.split(self.__destfilename))[0]
        print '\n destpath : ',self.__destpath


        return self.__srcpath, self.__srcbname, self.__destpath, self.__destbname


    def process(self):
        """ find out if its a file rename or a file move or move+rename """

        print "\n Connecting to Storage0 database .....\n"
        con = mdb.connect('localhost','storage0user','storage0roy','storage0')
        print "\n connection established \n"
        cur = con.cursor()

        uId, deviceId = self.getUserInfo()
        srcpath,srcfilename, destpath,destfilename = self.getFileInfo()


        if srcpath==destpath :
            print "\n file move type : RENAME "
            q = "UPDATE filesystem set filename = '%s' where uID = %d and deviceID = %d and path = '%s' and filename = '%s' " % (destfilename,uId,deviceId,srcpath,srcfilename)
            cur.execute(q)

        else :
            print "\n file move type : MOVE or MOVE+RENAME"
            q = "UPDATE filesystem set path = '%s' , filename = '%s' where uID = %d and deviceID = %d and path = '%s' and filename = '%s' " % (destpath,destfilename, uId,deviceId,srcpath,srcfilename)
            cur.execute(q)


        con.commit()
        cur.close()
        con.close()




class DirCreate:
    """Directory creation class"""

    def __init__(self,dirpath):
        self.__dirpath=dirpath


    def getUserInfo(self):

        return uID, deviceID



    def addDirEntry(self):
        print "\n Connecting to Storage0 database .....\n"
        con = mdb.connect('localhost','storage0user','storage0roy','storage0')
        print "\n connection established \n"
        cur = con.cursor()

        uId,deviceId = self.getUserInfo()
        dirname = os.path.basename(self.__dirpath)
        dirpath = os.path.split(self.__dirpath)[0]

        q="INSERT into filesystem (uID,deviceID,path,filename,filetype) values (%d,%d,'%s' ,'%s',%d)" % (uId,deviceId,dirpath,dirname,0)
        cur.execute(q)

        con.commit()
        cur.close()
        con.close()



class DirMove:
    """ Directory Movement class"""

    def __init__(self,srcpath,destpath):

        self.__srcpath = srcpath
        self.__destpath = destpath

    
    def getUserInfo(self):

        return uID, deviceID


    def getDirInfo(self):

        self.__srcdname = (os.path.split(self.__srcpath))[1]
        dname = self.__srcdname
        print '\n srcDname : ',dname
        self.__srcDpath = (os.path.split(self.__srcpath))[0]
        print '\n srcDpath : ',self.__srcDpath

        self.__destdname = (os.path.split(self.__destpath))[1]
        dname = self.__destdname
        print '\n destDname : ',dname
        self.__destDpath = (os.path.split(self.__destpath))[0]
        print '\n destDpath : ',self.__destdpath


        return self.__srcDpath, self.__srcdname, self.__destDpath, self.__destdname


    def process(self):
        """ find out if its a Dir rename or a Dir move or move+rename """

        print "\n Connecting to Storage0 database .....\n"
        con = mdb.connect('localhost','storage0user','storage0roy','storage0')
        print "\n connection established \n"
        cur = con.cursor()

        uId, deviceId = self.getUserInfo()
        srcDpath,srcdname, destDpath,destdname = self.getDirInfo()


        if srcDpath==destDpath :
            print "\n Dir move type : RENAME "
            # update the root level directory rename entry in filesystem table
            q = "UPDATE filesystem set filename = '%s' where uID = %d and deviceID = %d and path = '%s' and filename = '%s' " % (destDpath,destdname,uId,deviceId,srcDpath,srcdname)
            cur.execute(q)
            # update the files and subdirectories' path attribute in the filesystem table since the top level parent directory is renamed
            newpath = self.__srcpath+ '%'
            q = "SELECT path,filename from filesystem where uID = %d and deviceID = %d and path like '%s' " % (uId,deviceId,newpath)
            cur.execute(q)

            if cur.rowcount>0:
                rows = cur.fetchall()
                for row in rows:
                    path2,filename2 = row[0] , row [1]
                    l = len(srcDpath)
                    
                    fpath = os.path.join(path2[:l-1],destdname)
                    print " fpath : ", fpath
                    l2 = len(destdname)
                    finalpath = os.path.join(fpath,path2[l+l2:])
                    print "finalpath : ", finalpath
                    q = "UPDATE filesystem set path = '%s' where uID = %d and deviceID = %d and path = '%s' and filename = '%s' " % (finalpath,uId,deviceId,path2,filename2)
                    cur.execute(q)

        
        else :
            print "\n Dir move type : MOVE or MOVE+RENAME"
            q = "UPDATE filesystem set path = '%s', filename = '%s' where uID = %d and deviceID = %d and path = '%s' and filename = '%s' " % (destdname,uId,deviceId,srcDpath,srcdname)
            cur.execute(q)
            # update the files and subdirectories' path attribute in the filesystem table since the top level parent directory is renamed
            newpath = self.__srcpath+ '%'
            q = "SELECT path,filename from filesystem where uID = %d and deviceID = %d and path like '%s' " % (uId,deviceId,newpath)
            cur.execute(q)

            if cur.rowcount>0:
                rows = cur.fetchall()
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

        con.commit()
        cur.close()
        con.close()



 class DirDelete:
    """ Dirctory Deletion Class"""

    def __init__(self,dirpath) :

        self.__dirpath = dirpath


    def getUserInfo(self):

        return uID, deviceID


    def getDirInfo(self):

        self.__dname = (os.path.split(self.__dirpath))[1]
        bname = self.__bname
        print '\n bname : ',bname
        self.__path = (os.path.split(self.__dirpath))[0]
        print '\n path : ',self.__path

        return self.__path, self.__dname



    def del4mFilesystem(self):
        """ Delete the entry from table Filesystem"""

        print "\n Connecting to Storage0 database .....\n"
        con = mdb.connect('localhost','storage0user','storage0roy','storage0')
        print "\n connection established \n"
        cur = con.cursor()

        uId, deviceId = self.getUserInfo()
        path,dname = self.getFileInfo()

        # Delete the root level directory
        q= "DELETE from filesystem where uID = %d and deviceID = %d and path = '%s' and filename = '%s' and filetype = %d " % (uId,deviceId,path,dname,0)
        cur.execute(q)

        # Delete all files and sub directories' entries
        newpath=self.__dirpath+'%'

        q = "SELECT path,filename,filetype from filesystem where uID = %d and deviceID = %d and path like '%s' " % (uId,deviceId,newpath)
        cur.execute(q)

        if cur.rowcount>0 :
            rows = cur.fetchall()

            for row in rows:

                path2,filename2,filetype2 = row[0],row[1],row[2]
                # check if its a dirctory or a file and '0' means directory
                if filetype2==0:
                    q= "DELETE from filesystem where uID = %d and deviceID = %d and path = '%s' and filename = '%s' and filetype = '%s' " % (uId,deviceId,path2,filename2,filetype2)
                    cur.execute(q)
                else :
                    filepath= os.path.join(path,filename)
                    # Delete entry from filesystem table
                    fdel = fileSplit.FileDelete(filepath)
                    fdel.del4mFilesystem()



        con.commit()
        cur.close()
        con.close() 






def main():
    import sys

    if len(sys.argv)<4:
        sys.exit(usage())
        
    fsp = FileSplitter()
    
    fsp.parseOptions(sys.argv[1:])
    fsp.do_work()

if __name__=="__main__":
    main()
