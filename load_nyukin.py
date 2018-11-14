# command run: python load_nyukin.py ./nyukin elasticsearch_url
import csv
import os
import time
import sys
from multiprocessing import Pool
start_time = time.time()
requests = []

def readfile():
    for file in os.listdir(str(sys.argv[1])):
        fname = str(sys.argv[1]) + "/" + file
        params = "" #param request to elastic search server
        total = 0 #record of number
        count = 0
        with open(fname, 'rb') as csvfile:
            spamreader = csv.reader(csvfile)
            for row in spamreader:
                param = ', '.join(row)
                nk_kaisyacd = param.split(",")[0]
                nk_seqno = param.split(",")[1]
                nk_gyono = param.split(",")[2]
                kingaku = param.split(",")[15]
                authentification_cp_id = param.split(",")[49]
                params = params + "\"{ \\\"update\\\" : {\\\"_id\\\" : \\\"" + authentification_cp_id.strip() + "\\\"} }\n\"" \
                + "\"{ \\\"doc\\\": {" \
                    + "\\\"nk_kaisyacd\\\": \\\"" + nk_kaisyacd.strip() + "\\\"," \
                    + "\\\"nk_seqno\\\": \\\"" + nk_seqno.strip() + "\\\"," \
                    + "\\\"nk_gyono\\\": \\\"" + nk_gyono.strip() + "\\\"," \
                    + "\\\"nyukingaku\\\": " + kingaku.strip() \
                    + "\\\"nyukin_key\\\": \\\"" + nk_kaisyacd.strip() + nk_seqno.strip() + nk_gyono.strip() + "\\\""
                    + "}, \\\"doc_as_upsert\\\": true" \
                + "}\n\""
                count = count + 1
                if (count == 570):
                    requests.append("curl -XPOST '"
                        + str(sys.argv[2]) + "/supersonic/doc/_bulk"
                        + "' -H \"Content-Type: application/json\" -d "
                        + params)
                    total = total + count
                    count = 0
                    params = ""
                    print("Add params nyukin")

            if(count > 0):
                requests.append("curl -XPOST '"
                    + str(sys.argv[2]) + "/supersonic/doc/_bulk"
                    + "' -H \"Content-Type: application/json\" -d "
                    + params)
                total = total + count


def send_request(request):
    os.system(request)
    print("--- %s seconds ---" % (time.time() - start_time)) # calculate program run time

readfile()
p = Pool(40) #number of process
p.map(send_request, requests) # requests: array of params
