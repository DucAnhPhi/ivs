# based on https://realpython.com/python-sockets/#blocking-calls
# Heidelberg University
# Duc Anh Phi, Michael Tabachnik, Edgar Brotzmann
# Solution to Problem Set 1 IVS Exercise 4

def prepareReply(inData):
    DataStr = inData.decode()
    while len(inData)==1024:
        inData = conn.recv(1024)
        if not data:
            break
        DataStr += inData.decode()
    return str(len(DataStr.split())).encode()

import socket
HOST = "127.0.0.1"
PORT = 65432

with socket.socket (socket.AF_INET, socket.SOCK_STREAM) as s:
    # Avoid bind() exception: OSError: [Errno 48] Address already in use
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((HOST, PORT))
    s.listen()
    conn, addr = s.accept()
    with conn:
        print("Connected by", addr)
        while True:
            data = conn.recv(1024)
            if not data:
                break
            response = prepareReply(data)
            conn.sendall(response)