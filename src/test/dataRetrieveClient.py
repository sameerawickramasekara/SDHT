import socket
import json
import os
import sys
import time
import hashlib
from constants import *
import time


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

# dict_from_csv = {
#     "Luna_Seymour5082@sheye.org":"234"
# }
for key in dict_from_csv:
    
    validClient = False
    
    while validClient ==False:
        ClientSocket = socket.socket()
        # print("LOOP")
        try:
            ClientSocket.connect((DEFAULT_ROOT_HOST, DEFAULT_ROOT_PORT))

            msg = {"type":"getRandomNode"}
            ClientSocket.send(str.encode(json.dumps(msg)))
            data  = ClientSocket.recv(2048)
            # print("NODE",data)
            ClientSocket.close()
        except socket.error as e:
            print(str(e))
    

        client = json.loads(data)
        clienthost = client.get("host")
        clientport = client.get("port")

        if clientport == 1223 or  clientport == 1230:
            continue


        inputSocket = socket.socket()
        try:
            inputSocket.connect((clienthost, int(clientport)))
            validClient = True
        except socket.error as e:
            print(str(e))
        
    t = time.time()
    # time.sleep(0.2)
    ownKey=int(hashlib.sha256(str(key).encode()).hexdigest(),16) % 1048576
    # print("hashkey",ownKey)
    # hashdict[ownKey]=dict_from_csv[key]
    msg = {"type":"getItem","key":key}
    # print("data",key,ownKey)
    inputSocket.send(str.encode(json.dumps(msg)))
    data  = inputSocket.recv(2048)
    
    # print("datadsdfs",eval(data.decode("utf-8"))=="")
    try:
        value = False
        elapsed_time = time.time() - t
        if eval(data.decode("utf-8"))=="":
            value = False
        else :
            value = json.loads(json.loads(data)).get("value") ==dict_from_csv[key]

        print( value,elapsed_time)
        count = count + 1
        inputSocket.close()
    except Exception as e:
        print(False)
    

# print("data",data)
# print("HASH",len(hashdict))

