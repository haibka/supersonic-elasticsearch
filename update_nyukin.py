# command run: python load_nyukin.py ./nyukin elasticsearch_url
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
                nk_kaisyacd = param.split(",")[1]
                nk_seqno = param.split(",")[2]
                nk_gyono = param.split(",")[3]
                nyukin_key = nk_kaisyacd.strip() + nk_seqno.strip() + nk_gyono.strip()

                if (status == "I" or status == "U")
                    kingaku = param.split(",")[4]
                    authentification_cp_id = param.split(",")[5]
                    params_upsert = params_upsert + "\"{ \\\"update\\\" : {\\\"_id\\\" : \\\"" + authentification_cp_id.strip() + "\\\"} }\n\"" \
                    + "\"{ \\\"doc\\\": {" \
                        + "\\\"nk_kaisyacd\\\": \\\"" + nk_kaisyacd.strip() + "\\\"," \
                        + "\\\"nk_seqno\\\": \\\"" + nk_seqno.strip() + "\\\"," \
                        + "\\\"nk_gyono\\\": \\\"" + nk_gyono.strip() + "\\\"," \
                        + "\\\"nyukingaku\\\": " + kingaku.strip() \
                        + "}, \\\"doc_as_upsert\\\": true" \
                    + "}\n\""
                    count = count + 1
                else:
                    param_delete = param_delete + "\"{\\\"script\\\": {\\\"source\\\": \\\"ctx._source.remove(\\\\\"nyukingaku\\\\\")\\\"}," \
                      + "\\\"query\\\": {" \
                        + "\\\"term\\\": {" \
                          + "\\\"nyukin_key\\\": \\\"" + nyukin_key + "\\\"" \
                        + "}" \
                      + "}" \
                    + "}\""
                    requests_delete.append("curl -XPOST '"
                        + str(sys.argv[2]) + "/supersonic/doc/_update_by_query"
                        + "' -H \"Content-Type: application/json\" -d "
                        + param_delete)
                    param_delete = ""
                if (count == 570):
                    requests_upsert.append("curl -XPOST '"
                        + str(sys.argv[2]) + "/supersonic/doc/_bulk"
                        + "' -H \"Content-Type: application/json\" -d "
                        + params_upsert)
                    total = total + count
                    count = 0
                    params_upsert = ""
                    print("Add params nyukin")

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
