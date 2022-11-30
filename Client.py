import socket
import os
import sys
from multiprocessing import *
from Connections import *
import time
from filesplit.split import Split
import shutil
from Nodes import *
from Connections import *


class Client(object):

    def __int__(self):
        pass

    def Write(self, filename, num_workers):
        file = os.getcwd() + '/' + filename
        splitsize = int(os.path.getsize(file) / num_workers)

        outputpath = os.getcwd() + '/TempOutputs/'
        if not os.path.isdir(outputpath):
            os.mkdir(outputpath)

        split = Split(file, outputpath)
        split.bysize(splitsize)

        for i in range(1, num_workers + 1):
            w = WorkerNode(i + 2000)
            filenam = os.path.splitext(filename)[0]
            tempfilename = str(filenam) + '_' + str(i) + '.txt'
            w.Write(outputpath, tempfilename)

        shutil.rmtree(outputpath)

    def Read(self, filename, num_workers):
        outputfile = os.getcwd() + "/CombinedOutput.txt"
        f = open(outputfile, "a+")
        for i in range(1, num_workers + 1):
            w = WorkerNode(i + 2000)
            filenam = os.path.splitext(filename)[0]
            tempfilename = str(filenam) + '_' + str(i) + '.txt'
            tempcontent = w.Read(tempfilename)
            f.write(tempcontent)
        f.close()

    def MapFunc(self, num_workers, mapperpath):
        Write('input.txt', num_workers)
        filenam = os.path.splitext("input.txt")[0]
        for i in range(1, num_workers + 1):
            w = WorkerNode(i + 2000)
            w.mapfunc(mapperpath, filenam)

    def ShuffleFunc(self, num_workers):
        pass

    def RedFunc(self, num_workers, reducerpath):
        filenam = os.path.splitext("output.txt")[0]
        for i in range(1, num_workers + 1):
            w = WorkerNode(i + 2000)
            w.redfunc(reducerpath, filenam)


num_workers = int(sys.argv[1])
C = Client()
C.Write('input.txt', num_workers)
# C.Read('input.txt', num_workers)
