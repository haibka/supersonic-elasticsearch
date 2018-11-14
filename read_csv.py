import csv
import os
import time
from multiprocessing import Pool
start_time = time.time()
requests = []


with open("20181120_174530.txt", 'rb') as csvfile:
    spamreader = csv.reader(csvfile)
    print("dsd")
    for row in spamreader:
        param = ', '.join(row)
        print(param.split(",")[0])
