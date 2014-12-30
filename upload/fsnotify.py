

import os
import sys
import time
import fileSplit
import magicSync

from watchdog.observers import Observer
from watchdog.events import  *

#BASEDIR = os.path.abspath(os.path.dirname(__file__))
BASEDIR = '/home/cdot/Documents/Storage.0/MagicBox/'

class FileHandler(FileSystemEventHandler) :
 	""" When a file or dir changes, handle the events appropriately"""


 	def __init__(self):

 		self.__eventQueue = []

 	

 	def processCreation(self,event):
 		""" Process an file,dir creation even"""

 		print "\n event type : ",event.event_type
 		print "\n even src path : ", event.src_path
 		
 		if event.is_directory==False :
 			fsp = fileSplit.FileSplitter()
 			fsp.parseValues(event.src_path,4,0)
 			fsp.do_work()

 		else:
 			print "\n its a directory "
 			dce= fileSplit.DirCreate(event.src_path)
 			dce.addDirEntry()


 	def processDeletion(self,event):
 		""" Process a file,dir deletion event"""

 		print "\n event type : ",event.event_type
 		print "\n event src path : ",event.src_path

 		if event.is_directory==False :
 			fdel = fileSplit.FileDelete(event.src_path)
 			fdel.del4mFilesystem()

 		else :
 			print "\n its a directory "
 			ddel = fileSplit.DirDelete(event.src_path)
 			ddel.del4mFilesystem()



 	def processMovement(self,event) :
 		""" Process a file,dir on a movement event"""

 		print "\n event type : ",event.event_type
 		print "\n event src path : ",event.src_path
 		print "\n even dest_path : ",event.dest_path

 		if event.is_directory==False :
 			fmov = fileSplit.FileMove(event.src_path,event.dest_path)
 			fmov.process()

 		else :
 			print "\n its a directory "
 			dmov = fileSplit.DirMove(event.src_path,event.dest_path)
 			dmov.process()
 			


 	def processModification(self,event) :
 		""" Process a file,dir on mOdification event """

 		print "\n event type : ",event.event_type
 		print "\n event src path : ",event.src_path
 		#print "\n even dest_path : ",event.dest_path

 		fmodify = fileSplit.FileModiy(event.src_path,4)
 		success = fmodify.calHash()

 		if success ==0 :
 			# if the new file hash not found in m-serve then sync the new file and old file by only sending delta 
 			#magicSync()





 	

 	def on_created(self,event):
 		""" triggered on a file,dir creation"""

 		if isinstance(event,FileCreatedEvent): 
 			print "\n file created Event "
 			self.__eventQueue.append('fc')
 		elif isinstance(event,DirCreatedEvent) :
 			print "\n Dir Created Event"
 			self.__eventQueue.append('dc')
 		else :
 			print "\n not created event"




 		# if its a hidden file, bypass it, no need to process it
 		# because while a file modification , a hidden temporary file is crated for a split 
 		# second and delted and the new edited modified file is created again

 		fname = os.path.basename(event.src_path)

 		if (fname[0]=='.'):
 			print "\n filename with . : ",fname
 			print "\n skipping processCreation event"
 			#pass
 		elif (fname[len(fname)-1]=='~'):
 			print "\n filename with ~ : ",fname
 			print "\n skipping processCreation event"

 		else:
 			self.processCreation(event)


 	def on_deleted(self,event) :
 		""" triggered on a file,dir deletion"""

 		if isinstance(event,FileDeletedEvent): 
 			print "\n file Deleted Event "
 			self.__eventQueue.append('fd')
 		elif isinstance(event,DirDeletedEvent) :
 			print "\n Dir Deleted Event"
 			self.__eventQueue.append('dd')
 		else :
 			print "\n not Deleted event"

 		# if its a hidden or temporary file, bypass processDeletion event

 		fname = os.path.basename(event.src_path)

 		if (fname[0]=='.'):
 			print "\n filename with . : ",fname
 			print "\n skipping processDeletion event"
 			#pass
 		elif (fname[len(fname)-1]=='~'):
 			print "\n filename with ~ : ",fname
 			print "\n skipping processDeletion event"
 		else:
 			self.processDeletion(event)


 	def on_moved(self,event) :
 		""" triggered on a file,dire move or rename """

 		if isinstance(event,FileMovedEvent): 
 			print "\n file moved Event "
 			print "\nsrc path  : ", event.src_path
 			print "\ndest path  : ", event.dest_path
 			self.__eventQueue.append('fm')
 		elif isinstance(event,DirMovedEvent) :

 			print "\n Dir moved Event"
 			self.__eventQueue.append('dm')
 		else :
 			print "\n not moved event"

 		# if its a move of a hidden file or temporary file to another known existing file, handle it differently

 		sname = os.path.basename(event.src_path)
 		dname = os.path.basename(event.dest_path)

 		if (sname[0]=='.'):
 			print "\n src filename with . : ",sname
 			print "\n dest filename  : ",dname
 			print "\n skipping processMovement event"
 			#pass
 		elif (sname[len(sname)-1]=='~'):
 			print "\n src filename with ~ : ",sname
 			print "\n dest filename with . : ",dname
 			print "\n skipping processMovement event"
 		else:
 			self.processMovement(event)



 	def on_modified(self,event) :
 		""" triggered on a file,dir modification"""

 		if isinstance(event,FileModifiedEvent): 
 			print "\n file modified Event "
 			self.__eventQueue.append('fmod')
 		elif isinstance(event,DirModifiedEvent) :
 			print "\n Dir modified Event"
 			self.__eventQueue.append('dmod')
 		else :
 			print "\n not modified event"



 		#If modification of hidden file, handle it differently

 		if event.is_directory==False:

	 		fname = os.path.basename(event.src_path)

	 		if (fname[0]=='.'):
	 			print "\n filename with . : ",fname
	 			print "\n skipping processModification event"
	 			#pass
	 		elif (fname[len(fname)-1]=='~'):
	 			print "\n filename with ~ : ",fname
	 			print "\n skipping processModification event"

	 		else:
	 			l = len(self.__eventQueue)
	 			if self.__eventQueue[l-2]=='dmod' and self.__eventQueue[l-3]=='fc':
	 				del self.__eventQueue[:]
	 			else:
	 				self.processModification(event)

	 	else :
	 		print "\n directory modified event"
	 		print "\n skipping directory modified event always \n"


 	



if __name__ == "__main__" :

	fhandler = FileHandler()
 	observer = Observer()
 	print "\n Base Directory : ", BASEDIR
 	observer.schedule(fhandler,BASEDIR,recursive=True)
 	observer.start()

 	try :
 		while True:
 			time.sleep(1)
 	except KeyboardInterrupt :
 		observer.stop()

 	observer.join()




































# fsp = fileSplit.FileSplitter()
# fsp.parseValues(sys.argv[1],sys.argv[2],0)
# fsp.do_work()



