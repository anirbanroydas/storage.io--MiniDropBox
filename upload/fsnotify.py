

import os
import sys
import time
import fileSplit

from watchdog.observers import Observer
from watchdog.events import  FileSystemEventHandler

BASEDIR = os.path.abspath(os.path.dirname(__file__))

class FileHandler(FileSystemEventHandler) :
 	""" When a file or dir changes, handle the events appropriately"""

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


 	def processDeletion(self,event):
 		""" Process a file,dir deletion event"""

 		print "\n event type : ",event.event_type
 		print "\n event src path : ",event.src_path

 		if event.is_directory==False :
 			fdel = fileSplit.FileDelete(event.src_path)
 			fdel.del4mFilesystem()

 		else :
 			print "\n its a directory "



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

 




 	def on_created(self,event):
 		""" triggered on a file,dir creation"""

 		self.processCreation(event)


 	def on_deleted(self,event) :
 		""" triggered on a file,dir deletion"""

 		self.processDeletion(event)


 	def on_moved(self,event) :
 		""" triggered on a file,dire move or rename """

 		self.processMovement(event)



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



