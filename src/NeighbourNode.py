import socket
import json

class NeighbourNode:
    """
    This class is a representation for a relationship node connected to a chord node
    """
    def __init__(self,host=None,port=None,id=None):
        self.host = host
        self.port = port
        self.id = id
    
    def __repr__(self):
        return """
        id {id}
        """.format(id=self.id)

    def initConnection():
        print("init connecction")
    
    def join():
        print('join')

    def create():
        print('create')

    def retrive(self,key):
        ClientSocket = socket.socket()
        try:
            ClientSocket.connect((self.host, int(self.port)))
        except socket.error as e:
            print(str(e))
        ClientSocket.send(str.encode(json.dumps({
            "type":"retrive",
            "key":key
        })))
        Response = ClientSocket.recv(1024)
        return Response.decode('utf-8')
        

    def storeRelicatedData(self,key,value):
        print("Store called")
        ClientSocket = socket.socket()
        try:
            ClientSocket.connect((self.host, int(self.port)))
        except socket.error as e:
            print(str(e))
        ClientSocket.send(str.encode(json.dumps({
            "type":"storeReplicated",
            "key":key,
            "value":value
        })))    
        
    def store(self,key,value):
        ClientSocket = socket.socket()
        try:
            ClientSocket.connect((self.host, int(self.port)))
        except socket.error as e:
            print(str(e))
        ClientSocket.send(str.encode(json.dumps({
            "type":"store",
            "key":key,
            "value":value
        })))

    
    def serialize(self):
        return json.dumps({
            "host":self.host,
            "port":self.port,
            "id":self.id,
        })

    def getSuccessorList(self):
        
        ClientSocket = socket.socket()
        try:
            print('WHOST/PORT',self.host, int(self.port))
            ClientSocket.connect((self.host, int(self.port)))
        except socket.error as e:
            print(str(e))
        # msg = {"type":"find","payload":"23425"}
        ClientSocket.send(str.encode(json.dumps({
            "type":"getSuccessorList"
        })))

        Response = ClientSocket.recv(1024)
        responseData = json.loads(Response)

        if(responseData is not None):
            # print("SUCC LIST",responseData)
            list = []

            for item in responseData:
                node = NeighbourNode()
                node.deserialize(doc=item)

                list.append(node)
            return list
        return []
    
    def getAllDataForNode(self,key):    
        ClientSocket = socket.socket()
        try:
            ClientSocket.connect((self.host, int(self.port)))
        except socket.error as e:
            print(str(e))
        # msg = {"type":"find","payload":"23425"}
        ClientSocket.send(str.encode(json.dumps({
            "type":"getDatapoints",
            "key":key
        })))

        Response = ClientSocket.recv(1024)
        responseData = json.loads(Response)
        list = []
        if(responseData is not None):
            # print("SUCC LIST",responseData)
            for item in responseData:
                list.append({
                    "key":item.get("key"),
                    "value":item.get("value")
                })
        return list


    def deserialize(self,doc):
        obj = json.loads(doc)
        self.host = obj.get("host")
        self.port = obj.get("port")
        self.id = obj.get("id")
        
    def getPred(self):
        ClientSocket = socket.socket()
        print('Waiting for connection')
        try:
            ClientSocket.connect((self.host, int(self.port)))
        except socket.error as e:
            print(str(e))
        # msg = {"type":"find","payload":"23425"}
        ClientSocket.send(str.encode(json.dumps({
            "type":"getPred"
        })))

        Response = ClientSocket.recv(1024)
        responseData = json.loads(Response)

        if(responseData is not None):
            responseData = json.loads(responseData)
            return NeighbourNode(
                host=responseData.get("host"),
                port=responseData.get("port"),
                id=responseData.get("id")
            )
        return None

    def findSuccessor(self,newHashKey):
        ClientSocket = socket.socket()
        print('Waiting for connection')
        try:
            ClientSocket.connect((self.host, int(self.port)))
        except socket.error as e:
            print(str(e))
        # msg = {"type":"find","payload":"23425"}
        ClientSocket.send(str.encode(json.dumps({
            "type":"findSuccessor",
            "hashKey":newHashKey
        })))

        Response = ClientSocket.recv(1024)
        responseData = eval(Response.decode('utf-8'))

        return NeighbourNode(
            host=responseData.get("host"),
            port=responseData.get("port"),
            id=responseData.get("id")
        )


    def find(self,msg):
        ClientSocket = socket.socket()
        try:
            ClientSocket.connect((self.host, int(self.port)))
        except socket.error as e:
            print(str(e))
        Response = ClientSocket.recv(1024)
        if('Welcome to the server' == Response.decode('utf-8')):
            print("connected")
        
        # msg = {"type":"find","payload":"23425"}
        ClientSocket.send(str.encode(json.dumps(msg)))
        Response = ClientSocket.recv(1024)
        return Response.decode('utf-8')    
