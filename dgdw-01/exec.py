#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from datetime import datetime

database     = ["m3bg1"]
queries      = ["query-01", "query-02", "query-03", "query-04", "query-05", "query-06", "query-07" ] # .js extension
SUBDIR       = "-cbi2019"
NUM_EXEC     = 15

def atualDate():
   now = datetime.now()
   return "{:4d}{:02d}{:02d}-{:02d}{:02d}{:02d}".format(now.year,now.month,now.day,now.hour,now.minute,now.second)

def saveResultFile(directory, database, query, numExec, time):
    file = directory + os.sep + database + "-" + query + ".csv"
    f = open(file, 'a')
    f.write(database + ";" + query + ";" + str(numExec) + ";" + str(time) + ";\n" )
    f.close()
    return True
                
def directory():
    d = os.getcwd() +  os.sep + "result-time" + SUBDIR
    if not os.path.exists(d):
        os.makedirs(d)    
    d = os.getcwd()  +  os.sep + "result-query" + SUBDIR
    if not os.path.exists(d):
        os.makedirs(d)    

os.system("clear")		

directory()
print("=========================================================")	
print("EXPERIMENT")	
print("=========================================================")	
        
# mongo        
for numExec in range (NUM_EXEC):
    for q in queries:   
        for db in database:
            file = os.getcwd() +  os.sep + "result-query" + SUBDIR + os.sep + db + "-" + q + "-" + atualDate()              
            print (db + " > " + q + " > " + str(numExec))
            timeStart = datetime.now()          
            mongo = "mongo " + db + " " + db + os.sep + q + ".js > " + file
            print("   " + mongo)
            os.system(mongo) 
            timeEnd = datetime.now()
            saveResultFile(os.getcwd()+ os.sep + "result-time" + SUBDIR, db, q, numExec, timeEnd-timeStart )
            print("   START: " + str(timeStart) + "  END: " + str(timeEnd) + "  TOTAL: " + str(timeEnd - timeStart))
print("\nDone!")



