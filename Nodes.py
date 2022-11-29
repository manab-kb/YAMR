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

        #sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #result = sock.connect_ex(('127.0.0.1',port))
        #if result == 0:

        pworker = Process(target = self.Server, args =(self.port,))
        pworker.start()

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
        pass


a = MasterNode(3)


def Write(filename, num_workers):
    file = os.getcwd() + '/' + filename
    splitsize = int(os.path.getsize(file) / num_workers)

    outputpath = os.getcwd() + '/TempOutputs/'
    if not os.path.isdir(outputpath):
        os.mkdir(outputpath)
    
    split = Split(file, outputpath)
    split.bysize(splitsize)

    for i in range(1, num_workers+1):
        w = WorkerNode(i+2000)
        filenam = os.path.splitext(filename)[0]
        tempfilename = str(filenam) + '_' + str(i) + '.txt'
        w.Write(outputpath, tempfilename)

    shutil.rmtree(outputpath)


def Read(filename, num_workers):
    outputfile = os.getcwd() + "/CombinedOutput.txt"
    f = open(outputfile, "a+")
    for i in range(1, num_workers+1):
        w = WorkerNode(i+2000)
        filenam = os.path.splitext(filename)[0]
        tempfilename = str(filenam) + '_' + str(i) + '.txt'
        tempcontent = w.Read(tempfilename)
        f.write(tempcontent)
    f.close()


#Write('input.txt', 3)
Read('input.txt', 3)