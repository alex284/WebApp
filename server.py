'''
Simple tcp multi-threading server to play file to clients
Copyright: All Rights Reserved. 
Created by Alex Petukhov. Jun, 2015
'''

import sys
import traceback
import socketserver, time, datetime
import struct
import logging
import authentification_pb2  
import task1_heterogeneous_poisson_pb2
import pymongo
import dbutils

#These constants must be changed for our real database!!!!!!!!!!!!!!!!!!!!
BASE_NAME = 'dbtest'
TASK_NAME = 'task1' # Name of task MongoDB collection 
TASK_FILES = 'task_files'

def now():
    return time.ctime(time.time())

class ClientHandler(socketserver.BaseRequestHandler):   
  
    def send_msg(self, data):
        #Send message to client. Message length placed in first two bytes

        #send message length        
        self.request.send(struct.pack(">H", len(data)))    
        #send message
        self.request.send(data)

    def check_login(self, login_req):
        self.login = login_req.login
        self.password = login_req.enc_password
        reply = dbutils.checkAuth(self.login, self.password, self.server.database)
        return reply

    def auth_reply(self, authReply):
        #send to client information about connection status

        reply = authentification_pb2.LoginReply()
        reply.connection_status = authReply
        data = reply.SerializeToString()
        self.send_msg(data)

    def play_file(self):  
        speed = dbutils.getSpeed(file_name, self.server.database, TASK_FILES)
        if speed < 0: #something goes wrong in database, we cannot find correct speed for the file
            return
               
        event = task1_heterogeneous_poisson_pb2.Event()
        alen = len(file_data)
        #turn socket to non-blocking status
        self.request.setblocking(False)
        fin = False
        reply_len = 0
        delta = 0
        self.signals = []
        time_start = time.time()
        time_start = datetime.datetime.utcfromtimestamp(time_start)
        post = {'name':self.student_name, 'login':self.login, 'date_time':time_start}
        self.server.database[TASK_NAME].insert(post)
        time0 = time.time()
        for i in range(alen):
            t = time.time() 
            adj_data = file_data[i] / speed
            while time0 + adj_data > t:
                delta = time0 + adj_data - t                
                time.sleep(delta)
                t = time.time()

            event.server_timestamp_usec = int(time.time()*1000000)            
            print(event.server_timestamp_usec-1000000000000000)
            if i < alen-1:
                event.stream_end = False
            else:
                event.stream_end = True
            data = event.SerializeToString()
            self.send_msg(data)  
            reply_len = 0
            try:
                reply_len = struct.unpack(">H", self.request.recv(2))[0] 
            except Exception:
                pass
            if not reply_len: continue
            else:                 
                fin = True
                self.process_reply(reply_len)

        #turn socket to blocking status again
        self.request.setblocking(True)
        self.server.database[TASK_NAME].update(post, {'$set': {'time0':datetime.datetime.utcfromtimestamp(time0), 'end_time':datetime.datetime.utcfromtimestamp(time.time())}}, upsert=False)            
        if fin:
            for signal in self.signals:
                (tt, s) = signal
                self.server.database[TASK_NAME].update(post, {"$push":{"signals":{'date_time':tt, 'signal':s}}})
            pass
                   
        

    def process_reply(self, len):     
        #Process result from client    
        t = datetime.datetime.utcfromtimestamp(time.time())
        reply_data = self.request.recv(len)
        signal = task1_heterogeneous_poisson_pb2.Signal()
        signal.ParseFromString(reply_data)
        self.signals.append((t,signal.signal))           
        pass
        #Further processing needed

    def handle(self):
        print(self.client_address, now()) 
              
        try:            
            #read message length
            alen = struct.unpack(">H", self.request.recv(2))[0]             
            if not alen: return
            
            #read auth message
            data = self.request.recv(alen)
            if not data or len(data) < alen: return

            #parse auth message
            login_req = authentification_pb2.LoginRequest()
            login_req.ParseFromString(data)

            #check login, pass etc
            authReply, self.student_name = self.check_login(login_req)            

            #reply to client for auth message
            self.auth_reply(authReply)

            if authReply != authentification_pb2.LoginReply.OK:
                logger.info(u'Auth request from ' + self.student_name + ' FAILED') 
                return

            logger.info(u'Auth request from ' + self.student_name + ' OK')
            #start to play file    
            self.play_file()

            logger.info(u'Data has been streamed successfully for ' + self.student_name) 
            self.request.close()
        except Exception:
            logger.exception('')
            traceback.print_exc()
            self.request.close()


try:
    log_file_name = './log/' + datetime.datetime.now().strftime(TASK_NAME + '_%H_%M_%d_%m_%Y.log')    
    logger = logging.getLogger('server')
    hdlr = logging.FileHandler(log_file_name)
    formatter = logging.Formatter('%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr) 
    logger.setLevel(logging.DEBUG)

    #logging.basicConfig(format = u'%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s', level = logging.DEBUG)

    logger.info( u'Starting...' )

    db = pymongo.MongoClient("localhost", 27017)    
    file_name = db[BASE_NAME][TASK_FILES].find_one({'task':TASK_NAME})['file'] 
    
    file_data = []
    file_data = [int(d)/1000000.0 for d in open(file_name)]     
            
except Exception:
    traceback.print_exc()
    sys.exit(0)
try:
    host = ''
    port = 50007
    addr = (host, port)    
    server = socketserver.ThreadingTCPServer(addr, ClientHandler)
    server.database = db[BASE_NAME]
    server.serve_forever()
except Exception:
    traceback.print_exc()
    sys.exit(0)