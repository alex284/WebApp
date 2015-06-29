'''
Set of functions to handle database.
Copyright: All Rights Reserved. 
Created by Alex Petukhov. Jun, 2015
'''
import sys
import pymongo
import authentification_pb2 

#These constants must be changed for our real database!!!!!!!!!!!!!!!!!!!!
STUDETNS_COLLECTION = 'students'

def checkAuth(login, password, db):
    #checking login and password in database      
    user = db[STUDETNS_COLLECTION].find_one({'login':login, 'password':password})
    if user:
        res = authentification_pb2.LoginReply.OK
        name = user['name']
    else:
        res = authentification_pb2.LoginReply.BAD_LOGIN_OR_PASSWORD
        name = ''
    return res, name

def getSpeed(filename, db, collection_name):
    #get the speed for streaming from database
    task_files = db[collection_name]
    rec = task_files.find_one({'file':filename})
    if not rec: return -1
    return float(rec['speed'])
    