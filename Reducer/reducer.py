import os.path
import sys

path = sys.argv[1]
basepath = os.path.abspath(os.path.join(path, os.pardir))
outputpath = basepath + '/part-00000.txt'

f = open(path, "r")
f2 = open(outputpath, "a+")

content = f.read()
contentlist = content.split(" ")
contentset = set(contentlist)


for i in contentset:
    count = 0
    for j in contentlist:
        if i == j:
            count += 1
    f2.write(i + ' ' + str(count) + '\n')
