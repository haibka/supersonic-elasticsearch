# command run: python load_seikyu.py ./seikyu elasticsearch_url
import csv
import os
import time
import sys
from multiprocessing import Pool
start_time = time.time()
requests_upsert = []
requests_delete = []

def readfile():
    for file in os.listdir(str(sys.argv[1])):
        fname = str(sys.argv[1]) + "/" + file
        params_upsert = "" #param request to elastic search server
        param_delete = "" #param request to elastic search server
        total = 0 #record of number
        count = 0
        with open(fname, 'rb') as csvfile:
            spamreader = csv.reader(csvfile)
            for row in spamreader:
                param = ', '.join(row)
                status = param.split(",")[0]
                sk_kaisyacd = param.split(",")[1]
                sk_seqno = param.split(",")[2]
                sk_gyono = param.split(",")[3]
                if (status == "I"  or status = "U"):
                    seikyugaku = param.split(",")[4]
                    shop_code = param.split(",")[5]
                    np_transaction_id = param.split(",")[6]
                    params = params + "\"{ \\\"update\\\" : {\\\"_id\\\" : \\\"" + shop_code.strip() + np_transaction_id.strip() + "\\\"} }\n\"" \
                    + "\"{ \\\"doc\\\": {" \
                        + "\\\"sk_kaisyacd\\\": \\\"" + sk_kaisyacd.strip() + "\\\"," \
                        + "\\\"sk_seqno\\\": \\\"" + sk_seqno.strip() + "\\\"," \
                        + "\\\"sk_gyono\\\": \\\"" + sk_gyono.strip() + "\\\"," \
                        + "\\\"seikyugaku\\\": " + seikyugaku.strip() \
                        + "\\\"seikyu_key\\\": \\\"" + sk_kaisyacd.strip() + sk_seqno.strip() + sk_gyono.strip() + "\\\""\
                        + "}, \\\"doc_as_upsert\\\": true" \
                    + "}\n\""
                else:
                    param_delete = param_delete
                count = count + 1
                if (count == 570):
                    requests_upsert.append("curl -XPOST '"
                        + str(sys.argv[2]) + "/supersonic/doc/_bulk"
                        + "' -H \"Content-Type: application/json\" -d "
                        + params_upsert)
                    total = total + count
                    count = 0
                    params_upsert = ""
                    print("Add params seikyu")

            if(count > 0):
                requests_upsert.append("curl -XPOST '"
                    + str(sys.argv[2]) + "/supersonic/doc/_bulk"
                    + "' -H \"Content-Type: application/json\" -d "
                    + params_upsert)
                total = total + count


def send_request(request):
    os.system(request)
    print("--- %s seconds ---" % (time.time() - start_time)) # calculate program run time

readfile()
p = Pool(40) #number of process
p.map(send_request, requests_upsert) # requests: array of params
p.map(send_request, requests_delete) # requests: array of params
