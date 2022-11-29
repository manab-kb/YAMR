import socket
import os
from multiprocessing import *
from Connections import *
import time
from filesplit.split import Split

class WorkerNode(Connections):

    def __init__(self, port):
        self.port = port

        pworker = Process(target = self.Server, args =(self.port,))
        pworker.start()
        
        pathname = os.getcwd() + '/Storage' + str(self.port-2000)
        
        if not os.path.isdir(pathname):
            os.mkdir(pathname)
    
    def Write(self, filename, num_workers):
        file = os.getcwd() + '/' + filename
        splitsize = int(os.path.getsize(file) / num_workers)
        outputpath = os.getcwd() + '/Storage1'

        split = Split(file, outputpath)
        split.bysize(splitsize)

    def Read(self, filename):
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
    
    def stats(self):
        print('PPID:', os.getppid())
        print('PID:', os.getpid())
    
    def WriteFile(self, filename):
        self.WorkerNode.Write(filename, self.num_workers)

    def ReadFile(self, filename):
        pass

    def MapReduce(self):
        pass

    def Q(self):
        pass

a = MasterNode(3)
time.sleep(1000)
a.WriteFile('input.txt')