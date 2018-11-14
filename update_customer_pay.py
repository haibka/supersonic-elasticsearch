# command run: python update_customer_pay.py filename elasticsearch_url
import csv
import os
import time
import sys
from multiprocessing import Pool
start_time = time.time()
requests = []

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
            if(status == "I" or status == "U"):
                account_number = param.split(",")[2]
                receipts_date = param.split(",")[3]
                receipts_amount = param.split(",")[4]
                shop_code = param.split(",")[5]
                np_transaction_id = param.split(",")[6]
                ir_flg = param.split(",")[7]
                transfer_name = param.split(",")[8]
                bank_code = param.split(",")[9]
                branch_code = param.split(",")[9]
                tantocd = param.split(",")[10]
                action_id = param.split(",")[11]
                furikae_status_id = param.split(",")[12]
                azukari_status_id = param.split(",")[13]
                params = params + "\"{ \\\"update\\\" : {\\\"_id\\\" : \\\"" + id.strip() + "\\\"} }\n\"" \
                    + "\"{ \\\"doc\\\":{" \
                        + "\\\"customer_pay_id\\\": \\\"" + id.strip() + "\\\"," \
                        + "\\\"account_number\\\": \\\"" + account_number.strip() + "\\\"," \
                        + "\\\"receipts_date\\\": \\\"" + receipts_date.strip() + "\\\"," \
                        + "\\\"receipts_amount\\\": " + receipts_amount.strip() + "," \
                        + "\\\"shop_code\\\": \\\"" + shop_code.strip() + "\\\"," \
                        + "\\\"np_transaction_id\\\": \\\"" + np_transaction_id.strip() + "\\\"," \
                        + "\\\"ir_flg\\\": \\\"" + ir_flg.strip() + "\\\"," \
                        + "\\\"transfer_name\\\": \\\"" + transfer_name.strip() + "\\\"," \
                        + "\\\"bank_code\\\": \\\"" + bank_code.strip() + "\\\"," \
                        + "\\\"branch_code\\\": \\\"" + branch_code.strip() + "\\\"," \
                        + "\\\"tantocd\\\": \\\"" + tantocd.strip() + "\\\"," \
                        + "\\\"action_id\\\": \\\"" + action_id.strip() + "\\\"," \
                        + "\\\"azukari_status_id\\\": \\\"" + azukari_status_id.strip() + "\\\"," \
                        + "\\\"furikae_status_id\\\": \\\"" + furikae_status_id.strip() + "\\\"," \
                        + "\\\"otoiawasebango\\\": \\\"" + shop_code.strip() + np_transaction_id.strip() + "\\\"," \
                        + "\\\"azukari_histories\\\": []}, \\\"doc_as_upsert\\\" : true}" \
                    + "}\n\""
            else:
                params = params + "\"{\\\"update\\\": {\\\"_id\\\": \\\"" + id.strip() + "\\\", \\\"_retry_on_conflict\\\" : 3} }\n\"" \
					+ "\"{\\\"script\\\" : {\\\"source\\\": \\\"ctx._source.remove(\\\\\\\"otoiawasebango\\\\\\\"); " \
                    + "ctx._source.remove(\\\\\\\"receipts_date\\\\\\\"); ctx._source.remove(\\\\\\\"transfer_name\\\\\\\"); " \
                    + "ctx._source.remove(\\\\\\\"azukari_status_id\\\\\\\"); ctx._source.remove(\\\\\\\"receipts_amount\\\\\\\"); " \
                    + "ctx._source.remove(\\\\\\\"account_number\\\\\\\"); ctx._source.remove(\\\\\\\"ir_flg\\\\\\\"); " \
                    + "ctx._source.remove(\\\\\\\"bank_code\\\\\\\"); ctx._source.remove(\\\\\\\"branch_code\\\\\\\"); " \
                    + "ctx._source.remove(\\\\\\\"tantocd\\\\\\\"); ctx._source.remove(\\\\\\\"action_id\\\\\\\"); " \
                    + "ctx._source.remove(\\\\\\\"furikae_status_id\\\\\\\"); ctx._source.remove(\\\\\\\"shop_code\\\\\\\"); " \
                    + "ctx._source.remove(\\\\\\\"np_transaction_id\\\\\\\");\\\"}}\n\""
            count = count + 1
            if (count == 570):
                requests.append("curl -XPOST '"
                    + str(sys.argv[2]) + "/supersonic/doc/_bulk"
                    + "' -H \"Content-Type: application/json\" -d "
                    + params)
                count = 0
                params = ""
                print("Add params customer_pay")

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
