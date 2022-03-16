from threading import Thread
import hashlib
import time
import json
import logging
import os
from NeighbourNode import NeighbourNode
from constants import *

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

class NodeWorker(Thread):
    """
    Worker thread class invoked by the chord node
    """
    def __init__(self,
        relations,
        data,
        connection,
        ownKey,
        next,
        fingerTable,
        updateFingerTable,
        updateNext,
        successorList,
        updateDataStore
    ):
        Thread.__init__(self)
        
        self.relations = relations
        self.ownKey = ownKey
        self.data = data
        self.connection = connection
        self.next = next
        self.fingerTable = fingerTable
        self.updateFingerTable = updateFingerTable
        self.updateNext = updateNext
        self.updateDataStore = updateDataStore
        self.successorList = successorList
        self.logger = logging.getLogger("Worker Thread ["+str(self.ownKey)+"]")

    def getHashID(self,key):
        self.logger.info("Calculating hash key %s",key)
        result = hashlib.sha256(key.encode())
        id = int(result.hexdigest(),16)

        return id % CHORD_RING_SIZE
    
    def findClosestPrecedingNode(self,hashKey):
        for key in reversed(self.fingerTable):
            if self.inRange(self.relations.own.id,hashKey,self.fingerTable[key].id):
                return self.fingerTable[key]
        return self.relations.own
        

    def findSuccesor(self,newHashKey):
        self.logger.info("Finding closest successor node")
        if(self.inRange(
            node=self.ownKey,
            succesor=self.relations.succ.id,
            id=newHashKey)
        ):
            return self.relations.succ
        else:
            node = self.findClosestPrecedingNode(newHashKey)
            if(node.id == self.relations.own.id):
                return node
            return node.findSuccessor(newHashKey)
    
    def inRange(self,node,succesor,id):
        if(node < succesor):
            return node < id and id <= succesor
        return node < id or id<=succesor
    
        
    def updatePred(self,node):

        if(self.relations.pred is None or self.inRange(node=self.relations.pred.id,succesor=self.relations.own.id,id=node.id)):        
            self.relations.pred = node

    def run(self):
        """
        All messages go through the run method where each command is checked
        """
        self.logger.info("Starting worker node ")
        data = self.connection.recv(2048)
        if not data:
            self.connection.close()    
        msg = eval(data.decode('utf-8'))

        self.logger.info("Proccessing the command : %s",msg)
        if(msg.get("type") == 'find'):
            x = self.data.get(msg.get("key"))
        
        if(msg.get("type")== 'getPred'):            
            successorResponse =  self.relations.pred.serialize() if self.relations.pred!=None else None 
            self.connection.sendall(str.encode(json.dumps(successorResponse)))
            self.connection.close()
            self.logger.info("Successfully returned pred node ")
        
        elif(msg.get("type") == 'retrive'):
            key = msg.get("key")
            data = self.data[key]

            self.connection.sendall(str.encode(json.dumps({
                "value":data
            })))
            self.logger.info("Successfully returned data for key %s ",key)
            self.connection.close()        

        elif(msg.get("type")== 'store'):
            key = msg.get("key")
            value=msg.get("value")

            self.data[key] = value
            self.updateDataStore(self.data)

            if self.relations.succ is not None:
                self.relations.succ.storeRelicatedData(key=key,value=value)    

            if self.relations.pred is not None:
                self.relations.pred.storeRelicatedData(key=key,value=value)

            self.logger.info("Successfully stored data  for key %s and value %s",key,value)

        elif(msg.get("type")== 'storeReplicated'):
            key = msg.get("key")
            value=msg.get("value")

            self.data[key] = value
            self.updateDataStore(self.data)
             
        elif(msg.get("type") == 'getDatapoints'):            
            hashKey = msg.get("key")
            data = []
            for key,value in self.data.items():
                if(key <= hashKey):
                    data.append({
                        "key":key,
                        "value":value
                    })

            self.connection.sendall(str.encode(json.dumps(data)))
            self.connection.close() 
            self.logger.info("Successfully returned datapoints for successor %s ",hashKey)
            
        elif(msg.get("type")== 'addItem'):            
            key = msg.get("key")
            value=msg.get("value")
            hashKey = self.getHashID(key) 
            self.logger.info("Adding key %s value: %s ",key,value)
            successNode = self.findSuccesor(hashKey)
            successNode.store(key=hashKey,value=value)
        
        elif(msg.get("type")== 'getItem'):            
            key = msg.get("key")
            hashKey = self.getHashID(key) 
            self.logger.info("Getting value for key %s ",key)

            successNode = self.findSuccesor(hashKey)
            data = successNode.retrive(key=hashKey)        
            self.connection.sendall(str.encode(json.dumps(data)))
            self.connection.close()  
            
    
        elif(msg.get("type")== 'getSuccessorList'):           
            successorList =  [succ.serialize() for succ in self.successorList]
            self.connection.sendall(str.encode(json.dumps(successorList)))
            self.connection.close()            

        if(msg.get("type")=="findSuccessor"):
            hashKey = msg.get("hashKey")
            successorResponse = self.findSuccesor(hashKey).serialize()
            self.connection.sendall(str.encode(successorResponse))
            self.connection.close()

        elif(msg.get("type") == 'join'):
            hashKey = msg.get("hashKey")
            if(hashKey is not None):
                successorResponse = self.findSuccesor(hashKey).serialize()
                self.connection.sendall(str.encode(successorResponse))
                self.connection.close()
        elif(msg.get("type")=='notify'):
            doc = msg.get("value")
            predNode = NeighbourNode()
            predNode.deserialize(doc)

            self.updatePred(predNode)
            
            newNext = self.next +1
            if(newNext > 10):
                newNext = 1
            self.updateNext(newNext)
            
            self.fingerTable[newNext]=self.findSuccesor( 
                (int(self.relations.own.id) + 2**(newNext-1))
            )
            self.updateFingerTable(self.fingerTable)
            
        self.connection.close()
