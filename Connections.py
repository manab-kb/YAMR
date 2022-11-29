import socket
from multiprocessing import *

class Connections():

    def Server(self, PORT):
        #stats()
        HOST = "127.0.0.1"

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((HOST, PORT))
            s.listen()
            conn, addr = s.accept()
            with conn:
                print(f"WorkerNode: {addr}")
                while True:
                    data = conn.recv(1024)
                    if not data:
                        break
                    conn.sendall(data)

    def Client(self, PORT):
        HOST = "127.0.0.1"

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            s.sendall(b"Connected")
            data = s.recv(1024)
