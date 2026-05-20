import socket
import cv2 as cv
import json

class config:
    stream_host = "localhost"
    port = 6100

def connectTCP():
    TCP_IP = config.stream_host
    TCP_PORT = config.port
 
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)
    print("Waiting for connection on port {TCP_PORT}...")
    connection, address = s.accept()
    print("Connection to {address}")

    return connection
