""" FileSplitter -
Split a file into multiple small files


"""

import os, sys
import hashlib

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

    fmultiplier = 1048576

    def __init__(self):

        # cache filename
        self.__filename = ''
        # number of equal sized chunks
        self.__numchunks = 0
        # Size of each chunk
        self.__chunksize = 0
        # Optional postfix string for the chunk filename
        self.__postfix = ''
        # Program name
        self.__progname = "FileSplitter.py"
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
                self.__chunksize = int(value)*self.fmultiplier
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


        bname = (os.path.split(self.__filename))[1]
        # Get the file size
        fsize = os.path.getsize(self.__filename)
        print "file size : ",fsize
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

        # send_filehash_mserver(filehash)


        # Split file if no file level match

        f.seek(0)
        chunksz = self.__chunksize
        total_bytes = 0
        hashlist = []
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
        f.close()

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
