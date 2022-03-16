import socket
import random
import json
import sys
from constants import *

DEFAULT_HOST="127.0.0.1"
DEFAULT_PORT=65000

class RootNote:
    """
    The root node where a reference for each node in the ring is kept
    """

    sdht_nodes = []

    def  __init__(self, host=DEFAULT_HOST,port=DEFAULT_PORT):
        self.host = host
        self.port = port

        self.ServerSocket  = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.ServerSocket.bind((self.host,int(self.port)))
        except socket.error as e:
            print(str(e))

        self.startRootNode(serverSocket=self.ServerSocket)
    
    def startRootNode(self,serverSocket):
        serverSocket.listen(5)
        serverSocket.settimeout(10)
        print("Starting root node...")
        while True:
            try:          
                print("CLIENTS",self.sdht_nodes)
                client,_ = serverSocket.accept()
                data = client.recv(2048)
                if not data:
                    self.connection.close()    
                msg = eval(data.decode('utf-8'))

                respones = self.handleRequest(command = msg)

                print("GOT RESPONSE",respones)
                client.sendall(str.encode(json.dumps(respones)))
                if msg.get("type")=="joinRequest" :
                    print("JOIN",respones)
                    data = client.recv(2048)
                    msg = eval(data.decode('utf-8'))
                    respones = self.handleRequest(command = msg)
                    print("SEND",respones)
                    client.sendall(str.encode(json.dumps(respones)))
                print("Closing client",respones)
                client.close()

            except Exception as e:
                print(e)
                continue

    
    def handleRequest(self,command):
        print("HANDLE COMMAND",command)
        commandType = command.get("type")
        print("handle message : ",len(self.sdht_nodes))

        if commandType == "joinRequest":
            return self._join(command) 
        elif commandType == "joinConfirmation":
            return self._joinConfirm(command)
        elif commandType == "getRandomNode":
            return self._getRandomNode()

           
    def _getRandomNode(self):
        randomNode = random.choice(self.sdht_nodes)
        return {
                "host":randomNode["host"],
                "port":randomNode["port"]
        }

    def _joinConfirm(self,command):
        nodeHost = command.get("host")
        nodePort = command.get("port")
        
        self.sdht_nodes.append({
            "host":nodeHost,
            "port":nodePort
        })

        return {
            "status" : "ok"
        }

    def _join(self,command):

        if len(self.sdht_nodes) == 0:
            return {
                "initialNode":True
            }
        else:
            randomNode = random.choice(self.sdht_nodes)
            return {
                "host":randomNode["host"],
                "port":randomNode["port"]
            }        


if __name__=="__main__":
    params = sys.argv
    # sys.argv
    rootHost = DEFAULT_HOST
    rootPort = DEFAULT_PORT

    if len(params) == 3:
        rootHost = params[1]
        rootPort = params[2]
    
    if rootHost != None and rootPort!=None :
        RootNote(host=rootHost,port=rootPort)
    else:
        RootNote()


