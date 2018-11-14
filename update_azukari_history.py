# command run: python update_customer_pay.py filename elasticsearch_url
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import csv
import os
import time
import sys
from multiprocessing import Pool
start_time = time.time()
requests = []

def custom_memo(memo):
    strings = memo.splitlines()
    result = ""
    for a in strings:
        result = result + a + "\n"
    return result

def readfile():
    fname = str(sys.argv[1])
    params = "" #param request to elastic search server
    total = 0 #record of number
    count = 0
    with open(fname, 'rb') as csvfile:
        spamreader = csv.reader(csvfile)
        for row in spamreader:
            param = ', '.join(row)
            status = param.split(",")[0]
            id = param.split(",")[1]
            tantocd = param.split(",")[2]
            ir_flg = param.split(",")[3]
            action_id = param.split(",")[4]
            furikae_status_id = param.split(",")[5]
            azukari_status_id = param.split(",")[6]
            memo = param.split(",")[7]
            r_id = param.split(",")[8]
            r_ymd = param.split(",")[9]
            customer_pay_id = param.split(",")[10]
            if(status == "I" or status == "U"):
                params = params + "\"{\\\"update\\\" : {\\\"_id\\\" : \\\"" + customer_pay_id.strip() + "\\\",\\\"_retry_on_conflict\\\" : 3}}\n\"" \
                    + "\"{\\\"scripted_upsert\\\": true,\\\"script\\\" : {\\\"source\\\": \\\"if (ctx._source.azukari_histories==null) ctx._source.azukari_histories = [];" \
                    + "ctx._source.azukari_histories.add(params.new)\\\",\\\"params\\\" : {\\\"new\\\" : {" \
                    + "\\\"id\\\": \\\"" + id.strip() + "\\\"," \
                    + "\\\"tantocd\\\": \\\"" + tantocd.strip() + "\\\"," \
                    + "\\\"ir_flg\\\": \\\"" + ir_flg.strip() + "\\\"," \
                    + "\\\"action_id\\\":\\\"" + action_id.strip() + "\\\"," \
                    + "\\\"furikae_status_id\\\": \\\"" + furikae_status_id.strip() + "\\\"," \
                    + "\\\"azukari_status_id\\\": \\\"" + azukari_status_id.strip() + "\\\"," \
                    + "\\\"memo\\\": \\\"" + custom_memo(memo.strip()) + "\\\"," \
                    + "\\\"r_id\\\": \\\"" + r_id.strip() + "\\\"," \
                    + "\\\"r_ymd\\\": \\\"" + r_ymd.strip() + "\\\"}}}," \
                    + "\\\"upsert\\\" : {\\\"azukari_histories\\\" : []}}\n\""
            else:
                params = params + "\"{\\\"update\\\": {\\\"_id\\\": \\\"" + customer_pay_id.strip() + "\\\", \\\"_retry_on_conflict\\\" : 3}}\n" \
                    + "\"{\\\"script\\\": {\\\"lang\\\": \\\"painless\\\"," \
                    + "\\\"source\\\": \\\"ctx._source.azukari_histories = ctx._source.azukari_histories.stream().filter(x->x.id != params.id).collect(Collectors.toList())\\\"," \
                    + "\\\"params\\\": {\\\"id\\\": \\\"" + id.strip() + "\\\"}}}\n\""
            count = count + 1
            if (count == 570):
                requests.append("curl -XPOST '"
                    + str(sys.argv[2]) + "/supersonic/doc/_bulk"
                    + "' -H \"Content-Type: application/json\" -d "
                    + params)
                count = 0
                params = ""
                print("Add params azukari_history")

        if(count > 0):
            requests.append("curl -XPOST '"
                + str(sys.argv[2]) + "/supersonic/doc/_bulk"
                + "' -H \"Content-Type: application/json\" -d "
                + params)
            count = 0
            params = ""


def send_request(request):
    os.system(request )
    print("--- %s seconds ---" % (time.time() - start_time)) # calculate program run time

readfile()
p = Pool(40) #number of process
p.map(send_request, requests) # requests: array of params
