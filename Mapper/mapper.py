import os.path
import sys

path = sys.argv[1]
basepath = os.path.abspath(os.path.join(path, os.pardir))
outputpath = basepath + '/Output.txt'

f = open(path, "r")
f2 = open(outputpath, "a+")

content = f.read()
contentlist = content.split(" ")


for i in contentlist:
    print(i + ' ,1', f2)
