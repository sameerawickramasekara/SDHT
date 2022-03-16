import socket
import json
import os
import sys
import time
import hashlib
from constants import *


host = "127.0.0.1"
port = 65000

import csv
dict_from_csv = {}

DEFAULT_ROOT_HOST = '127.0.0.1'
DEFAULT_ROOT_PORT = 65000

with open('dummydata.csv', mode='r') as inp:
    reader = csv.reader(inp)
    print("READER")
    print(reader)
    for rows in reader:
        dict_from_csv[rows[0]] = rows[1]
    
## get node



count = 0
hashdict = {}

for key in dict_from_csv:
    ClientSocket = socket.socket()
    try:
        ClientSocket.connect((DEFAULT_ROOT_HOST, DEFAULT_ROOT_PORT))
    except socket.error as e:
        print(str(e))
    msg = {"type":"getRandomNode"}
    ClientSocket.send(str.encode(json.dumps(msg)))
    data  = ClientSocket.recv(2048)
    ClientSocket.close()

    client = json.loads(data)
    clienthost = client.get("host")
    clientport = client.get("port")

    inputSocket = socket.socket()
    try:
        inputSocket.connect((clienthost, int(clientport)))
    except socket.error as e:
        print(str(e))
    
    # time.sleep(0.2)
    ownKey=int(hashlib.sha256(str(key).encode()).hexdigest(),16) % 1048576
    # print("hashkey",ownKey)
    hashdict[ownKey]=dict_from_csv[key]
    msg = {"type":"addItem","key":key,"value":dict_from_csv[key]}
    inputSocket.send(str.encode(json.dumps(msg)))
    print("added", str(count))
    count = count + 1
    inputSocket.close()
    # data  = inputSocket.recv(2048)

# print("data",data)
print("HASH",len(hashdict))

