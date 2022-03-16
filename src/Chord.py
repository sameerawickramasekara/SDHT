import socket
import os
import sys
from _thread import *
from NodeWorker import NodeWorker
from NeighbourNode import NeighbourNode
import hashlib
import json
import logging
from PeriodicWorker import PeriodicWorker
from NodeState import NodeState
from threading import Timer
from  constants import *

DEFAULT_ROOT_HOST = '127.0.0.1'
DEFAULT_ROOT_PORT = 65000

host = "127.0.0.1"
port = 1227

ownKey=int(hashlib.sha256(str(host+":"+str(port)).encode()).hexdigest(),16) % CHORD_RING_SIZE
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

class Chord:
    """
    Main object associated with a node in the DHT

    """    
    def __init__(self,
        host,
        port,
        rootHost,
        rootPort,
        relations=None,
        first=False,
        successorList=None
    ):
                
        self.host = host
        self.port = port
        self.rootHost = rootHost
        self.rootPort = rootPort
        self.first=first
        self.id = int(hashlib.sha256(str(host+":"+str(port)).encode()).hexdigest(),16) % CHORD_RING_SIZE
        self.fingerTable={}        
        self.next=0

        self.logger = logging.getLogger("Chord Node ["+str(self.id)+"]")
        if successorList is None:
            self.successorList=[]
        else:
            self.successorList = successorList

        if relations is None:
            self.relations=NodeState(
                    own = NeighbourNode(host,port,self.id)
            )
        else:
            self.relations = relations                   
        self.data={}
        self.logger.info("Initializing node  host : %s port : %s",host,port)
    

    def updateRelations(self,newRelations):
        self.logger.info("Updating relations : %s",newRelations)
        self.relations = newRelations
    
    def updateFingerTable(self,newFingerRable):
        self.logger.info("Updating Finger table : %s",newFingerRable)        
        self.fingerTable = newFingerRable
    
    def updateNext(self,newNext):
        self.next = newNext
    
    def onStabalizerStop(self):
        Timer(5, PeriodicWorker(relations=self.relations,
                onStop=self.onStabalizerStop,
                updateRelations=self.updateRelations,
                updateSuccessorList=self.updateSuccessorList,
                successorList=self.successorList
            ).run
        ).start()


    def initiateJoin(self):
        """
        Start the chord node by doing the following
            - Connect to root node and get an entry point for the chord ring
            - If there is no nodes in the ring , initiate as the starting node
        """
        self.logger.info("Requesting to join ring ")
        rootClientSocket = socket.socket()
        try:
            rootClientSocket.connect((self.rootHost, int(self.rootPort)))
        except socket.error as e:
            print(str(e))
        rootClientSocket.send(str.encode(json.dumps({
            "type":"joinRequest",
        })))
        rootResponse = rootClientSocket.recv(1024)
        self.logger.info("Chord ring entry point %s",rootResponse.decode('utf-8'))

        if rootResponse is not None :
            res = json.loads(rootResponse)
            
            isFirstNode = res.get("initialNode")
            if isFirstNode:
                self.logger.info("Initializing as the first node..")
                self.first = True
                self.relations = NodeState(
                        own=NeighbourNode(self.host,self.port,id=self.id) ,   
                        succ=NeighbourNode(self.host,self.port,id=self.id),
                        pred=None            
                    )
                
                rootClientSocket.send(str.encode(json.dumps({
                    "type":"joinConfirmation",
                    "host":self.host,
                    "port":self.port
                })))

                rootClientSocket.close()
                self.startListningtoPort()
            else:
                #Join the ring by calling the chord node with join message
                self.logger.info("Calling entry node..")
                joinHost = res.get("host")
                joinPort = res.get("port")

                ClientSocket = socket.socket()
                try:
                    ClientSocket.connect((joinHost, joinPort))
                except socket.error as e:
                    print(str(e))

                #Chord ring node returns the hash key for node and the successor node
                msg = {"type":"join","hashKey":self.id}

                ClientSocket.send(str.encode(json.dumps(msg)))
                Response = ClientSocket.recv(1024)

                if Response is not None:
                    self.logger.info("Successor node recieved : %s",Response.decode('utf-8'))
                    # set own successor node
                    succ = NeighbourNode()
                    succ.deserialize(Response)
                    self.relations.succ = succ

                    # Ask for relevent key value pairs from successor node
                    data = succ.getAllDataForNode(key=self.relations.own.id)
                    self.logger.info("Transferred data from successor node : %s",data)
                    for item in data:
                        self.data[item.get("key")]=item.get("value")
                    
                    #set finger table first entry to succ
                    self.fingerTable[1] = succ

                    #Get successor list form the successor node
                    list = succ.getSuccessorList() if succ.id != self.id else []
                    self.updateSuccessorList(list)

                    #Send the confirmation msg to root node so it can add this node as a ring node
                    rootClientSocket.send(str.encode(json.dumps({
                        "type":"joinConfirmation",
                        "host":self.host,
                        "port":self.port
                    })))

                    rootClientSocket.close()     
                    self.logger.info("Successfully completed initialization..")
                    self.startListningtoPort()

        
    def updateDataStore(self,data):
        self.data = data

    def updateSuccessorList(self,list):
        newAdd = [self.relations.succ]
        newAdd.extend(list)
        successors = []
        for i in range(len(newAdd)):
            if(i<4):
                successors.append(newAdd[i])

        self.successorList = successors

    def start(self):
        """
        Main Entry point for the chord node
        """
        self.initiateJoin()

    def startListningtoPort(self):
        """
        This method listens for incoming connections in a continous loop and
        delegate handling of each message to a new Thread
        """
        ServerSocket  = socket.socket()
        self.logger.info("Listning to incoming requests...")
        try:
            ServerSocket.bind((self.host,int(self.port)))
        except socket.error as e:
            print(str(e))
        self.logger.info("Waiting for a connection")
        ServerSocket.listen(5)

        # Start the stabalization thread in the background
        stabilizer = PeriodicWorker(
            relations=self.relations,
            onStop=self.onStabalizerStop,
            updateRelations=self.updateRelations,
            updateSuccessorList=self.updateSuccessorList ,
            successorList=self.successorList
            )
        stabilizer.start()

        while True:

            try:
                self.logger.info("Node Relations: %s",self.relations)
                self.logger.info("Node Data count: %s",len(self.data))
                
                Client,address = ServerSocket.accept()
                self.logger.info("New Connection with : %s",address)

                #initiate a worker thread to handle the request
                worker = NodeWorker(
                    relations=self.relations,
                    data=self.data,
                    connection=Client,
                    ownKey=self.id,
                    updateFingerTable=self.updateFingerTable,
                    updateNext=self.updateNext,
                    next=self.next,
                    fingerTable=self.fingerTable,
                    successorList=self.successorList,
                    updateDataStore=self.updateDataStore                    
                )
                self.logger.info("Starting worker thread to handle requests from : %s",address)
                worker.start()

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(e)
                continue


        ServerSocket.close()

def main(rootHost = DEFAULT_ROOT_HOST, rootPort = DEFAULT_ROOT_PORT , ownPort=port):    
    node = Chord(
        rootHost = rootHost,
        rootPort = rootPort, 
        port = ownPort, 
        host = host
    )
    node.start()


if __name__=="__main__":
    params = sys.argv
    print(str(sys.argv))
    rootHost = params[1]
    rootPort = params[2]
    ownPort = params[3]

    if host != None and port != None :
        main(rootHost=rootHost,rootPort=rootPort,ownPort=int(ownPort))
    else:
        main()

    


