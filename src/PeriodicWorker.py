from threading import Thread
import time
from utils import inRange
import  socket
from NeighbourNode import NeighbourNode
import json
import os
import logging

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

class PeriodicWorker(Thread):
    """
    Worker thread responsible for running the stabilization algorith periodically
    """

    def __init__(self,relations,onStop,updateRelations,updateSuccessorList,successorList):
        Thread.__init__(self)
        self.relations = relations
        self.onStop = onStop
        self.updateRelations = updateRelations
        self.updateSuccessorList = updateSuccessorList
        self.successorList = successorList
        self.logger = self.logger = logging.getLogger("Stabalizer Thread ["+str(self.relations.own.id)+"]")


    def run(self):       
        time.sleep(5)
        self.logger.info("Running stabalizer function...")
        self.stabalize()
    
    def findFirstAvailableSuccessor(self):
        for succ in self.successorList:
            ClientSocket = socket.socket()
            try:
                ClientSocket.connect((succ.host, succ.port))
            except socket.error as e:
                print(str(e))
                continue
            msg={"type":"dummy"}
            ClientSocket.send(str.encode(json.dumps(msg)))     
            ClientSocket.close()
            return succ
        return self.relations.own


    def stabalize(self):   
        newSucc = self.findFirstAvailableSuccessor()

        self.relations.succ = newSucc
        self.updateRelations(self.relations)

        succ = self.relations.succ
        own = self.relations.own

        if succ is not None:
            v = succ.getPred()
            if(v is not None and inRange(
                node = own.id,
                succesor=succ.id,
                id=v.id
            )):
                self.relations.succ = v
                self.updateRelations(self.relations)
            
            self.notify(self.relations.succ)
            
            list = self.relations.succ.getSuccessorList()
            self.updateSuccessorList(list)
        
        self.checkPred()
        self.onStop()


    def checkPred(self):
        if self.relations.pred != None:
            ClientSocket = socket.socket()
            try:
                ClientSocket.connect((self.relations.pred.host, self.relations.pred.port))
            except Exception as e:
                print(str(e))
                self.relations.pred=None     
                self.updateRelations(self.relations)      
            msg={"type":"dummy"}
            ClientSocket.send(str.encode(json.dumps(msg)))     



    def notify(self,node):
        if node is not None:
            ClientSocket = socket.socket()
            try:
                ClientSocket.connect((node.host, node.port))
            except socket.error as e:
                print(str(e))

            msg={"type":"notify","value":self.relations.own.serialize() }
            ClientSocket.send(str.encode(json.dumps(msg)))


