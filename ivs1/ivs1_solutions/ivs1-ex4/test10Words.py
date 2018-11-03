# based on https://realpython.com/python-sockets/#blocking-calls
# Heidelberg University
# Duc Anh Phi, Michael Tabachnik, Edgar Brotzmann
# Solution to Problem Set 1 IVS Exercise 4

import socket
import textData

HOST = "127.0.0.1"
PORT = 65432

inputStr = textData.TEXT10
with socket.socket (socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    s.sendall(inputStr.encode())
    data = s.recv(1024)
print("Received", repr(data.decode()))