import os.path
import sys

with open(os.path.dirname(__file__) + '/flights_data.csv', 'w') as output:
    sys.stdout = output

    with open(os.path.dirname(__file__) + "/../ontime/2016_1.csv") as fp:
       header = fp.readline()
       print("ID," + header, end = '')
       cnt = 0
       line = fp.readline()
       while cnt < 100:
           print(str(cnt) + "," + line, end = '')
           line = fp.readline()
           cnt += 1
