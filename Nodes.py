import socket
import os
from multiprocessing import *
from Connections import *
import time
from filesplit.split import Split
import shutil


class WorkerNode(Connections):

    def __init__(self, port):
        self.port = port

        pworker = Process(target = self.Server, args =(self.port,))
        pworker.start()

        paths = os.getcwd()
        self.paths = paths
        pathname = os.getcwd() + '/Storage' + str(self.port-2000)
        self.pathname = pathname

        if not os.path.isdir(pathname):
            os.mkdir(pathname)
    
    def Write(self, filepath, filename):
        newpath = self.pathname + '/' + filename
        filepath = filepath + filename
        os.rename(filepath, newpath)

    def Read(self, filename):
        file = self.pathname + '/' + filename
        f = open(file, "r")
        content = f.read()
        f.close()
        os.remove(file)
        return content

    def mapfunc(self, mapperpath, filenam):
        temppath = os.path.abspath(os.path.join(mapperpath, os.pardir))
        os.chdir(temppath)
        tempinput = temppath + '/' + filenam + '_' + str(self.port - 2000) + '.txt'
        os.system(f"python3 mapper.py -{tempinput}".format(tempinput = tempinput))
        os.chdir(self.paths)

    def shuffunc(self):
        pass

    def redfunc(self, reducerpath, filenam):
        tempinput = self.pathname + '/' + filenam + '.txt'
        temppath = os.path.abspath(os.path.join(reducerpath, os.pardir))
        os.chdir(temppath)
        os.system(f"python3 reducer.py -{tempinput}".format(tempinput=tempinput))
        os.chdir(self.paths)


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

    def Q(self):
        # Use process.start() and process.join() to schedule
        pass
