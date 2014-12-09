""" FileSplitter -
Split a file into multiple small files


"""

import os, sys
import hashlib
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

    __fmultiplier = 1048576
    __commitQlist = []
    __needblocks = []
    __blockdict = {}

    def __init__(self):

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

    def parseOptions(self, args):

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
                chunkf = file(chunkfilename, 'wb')
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

def main():
    import sys

    if len(sys.argv)<4:
        sys.exit(usage())
        
    fsp = FileSplitter()
    
    fsp.parseOptions(sys.argv[1:])
    fsp.do_work()

if __name__=="__main__":
    main()
