import socket
import os
from multiprocessing import *
from Connections import *

class WorkerNode(Connections):

    def __init__(self, port):
        self.port = port

        pworker = Process(target = self.Server, args =(self.port,))
        pworker.start()
        
        pathname = os.getcwd() + '/Storage' + str(self.port-2000)
        
        if not os.path.isdir(pathname):
            os.mkdir(pathname)
    
    def Write():
        pass

    def Read():
        pass


class MasterNode(Connections):

    def __init__(self, num_workers):
        self.num_workers = num_workers

        for i in range(1, self.num_workers+1):
            port = 2000 + i
            self.WorkerNode = WorkerNode(port)
        
        pmain = Process(target = self.Server, args =(2000,))
        pmain.start()

        for i in range(1,self.num_workers+1):
            self.Client(2000+i)
    
    def stats():
        print('PPID:', os.getppid())
        print('PID:', os.getpid())

a = MasterNode(3)
