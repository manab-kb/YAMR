import datetime
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
import datetime

fout = open("./Logs/log.txt", "a+")
sys.stderr = open("./Logs/errors.txt", "w")


class Client(object):

    def __int__(self):
        pass

    def Write(self, filename, num_workers):
        file = filename
        splitsize = int(os.path.getsize(file) / num_workers)

        outputpath = os.getcwd() + '/TempOutputs/'
        if not os.path.isdir(outputpath):
            os.mkdir(outputpath)

        split = Split(file, outputpath)
        split.bysize(splitsize)

        for i in range(1, num_workers + 1):
            m = MasterNode(num_workers)
            fout.write(f"MasterNode 127.0.0.1:2000 Active\n")
            w = WorkerNode(i + 2000)
            portnum = str(i+2000)
            fout.write(f"WorkerNode 127.0.0.1:{portnum} Active\n".format(portnum = portnum))
            path, ext = os.path.splitext(filename)
            filenam = path.split('/')
            filenam = filenam[-1]
            tempfilename = str(filenam) + '_' + str(i) + '.txt'
            w.Write(outputpath, tempfilename)

        fout.write("\nWRITE operation complete\n")
        shutil.rmtree(outputpath)

    def Read(self, filename, num_workers, inputpath):
        outputfile = filename
        f = open(outputfile, "a+")
        for i in range(1, num_workers + 1):
            m = MasterNode(num_workers)
            fout.write(f"MasterNode 127.0.0.1:2000 Active\n")
            w = WorkerNode(i + 2000)
            portnum = str(i + 2000)
            fout.write(f"WorkerNode 127.0.0.1:{portnum} Active\n".format(portnum=portnum))
            path, ext = os.path.splitext(inputpath)
            filenam = path.split('/')
            filenam = filenam[-1]
            tempfilename = str(filenam) + '_' + str(i) + '.txt'
            tempcontent = w.Read(tempfilename)
            f.write(tempcontent)

        f.close()
        fout.write("\nREAD operation complete\n")

    def MapFunc(self, num_workers, mapperpath, filename):
        self.Write(filename, num_workers)
        path, ext = os.path.splitext(filename)
        filenam = path.split('/')
        filenam = filenam[-1]
        for i in range(1, num_workers + 1):
            m = MasterNode(num_workers)
            fout.write(f"MasterNode 127.0.0.1:2000 Active\n")
            w = WorkerNode(i + 2000)
            portnum = str(i + 2000)
            fout.write(f"WorkerNode 127.0.0.1:{portnum} Active\n".format(portnum=portnum))
            w.mapfunc(mapperpath, filenam)

        fout.write("\nMAP operation complete\n")

    def ShuffleFunc(self, num_workers):
        pass

    def RedFunc(self, num_workers, reducerpath):
        filenam = os.path.splitext("Output.txt")[0]
        for i in range(1, num_workers + 1):
            m = MasterNode(num_workers)
            fout.write(f"MasterNode 127.0.0.1:2000 Active\n")
            w = WorkerNode(i + 2000)
            portnum = str(i + 2000)
            fout.write(f"WorkerNode 127.0.0.1:{portnum} Active".format(portnum=portnum))
            w.redfunc(reducerpath, filenam)

        fout.write("\nREDUCE operation complete\n")


task = sys.argv[1]
if task == "write":    # Example: python3 Client.py write /home/usrnam/Desktop/YAMR-main/input.txt 5
    # Replace usrnam by your username
    C = Client()
    inputpath = sys.argv[2]
    num_workers = int(sys.argv[3])
    fout.write("\nWRITE OPERATION SCHEDULED AT " + str(datetime.datetime.now()) + "\n\n")
    C.Write(inputpath, num_workers)
    sys.exit()

if task == "read":
    # Example:  python3 Client.py write /home/usrnam/Desktop/YAMR-main/CombinedOutputs.txt 5 /home/manabkb/Desktop/YAMR-main/input.txt
    # Replace usrnam by your username
    C = Client()
    outputpath = sys.argv[2]
    num_workers = int(sys.argv[3])
    inputpath = sys.argv[4]
    fout.write("\nREAD OPERATION SCHEDULED AT " + str(datetime.datetime.now()) + "\n\n")
    C.Read(outputpath, num_workers, inputpath)
    sys.exit()

if task == "mapreduce":
    # Example: python3 Client.py mapreduce /home/usrnam/Desktop/YAMR-main/Mapper/mapper.py /home/usrnam/Desktop/YAMR-main/Reducer/reducer.py 5 /home/usrnam/Desktop/YAMR-main/input.txt
    # Replace usrnam by your username
    C = Client()
    mapperpath = sys.argv[2]
    reducerpath = sys.argv[3]
    num_workers = int(sys.argv[4])
    filename = sys.argv[5]
    fout.write("\nMAPREDUCE OPERATION SCHEDULED AT " + str(datetime.datetime.now()) + "\n\n")
    C.MapFunc(num_workers, mapperpath, filename)
    C.RedFunc(num_workers, reducerpath)
    sys.exit()

fout.close()
