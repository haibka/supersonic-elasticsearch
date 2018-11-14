# command run: python load_customer_pay.py ./customer_pay elasticsearch_url
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
                id = param.split(",")[0]
                account_number = param.split(",")[8]
                receipts_date = param.split(",")[10]
                receipts_amount = param.split(",")[12]
                shop_code = param.split(",")[15]
                np_transaction_id = param.split(",")[16]
                ir_flg = param.split(",")[18]
                transfer_name = param.split(",")[22]
                bank_code = param.split(",")[25]
                branch_code = param.split(",")[26]
                tantocd = param.split(",")[35]
                action_id = param.split(",")[36]
                furikae_status_id = param.split(",")[37]
                azukari_status_id = param.split(",")[38]
                params = params + "\"{ \\\"update\\\" : {\\\"_id\\\" : \\\"" + id + "\\\"} }\n\"" \
                    + "\"{ \\\"doc\\\":{" \
                        + "\\\"customer_pay_id\\\": \\\"" + id + "\\\"," \
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
                        + "\\\"azukari_histories\\\": [] }" \
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
                    print("Add params customer_pay")

            if(count > 0):
                requests.append("curl -XPOST '"
                    + str(sys.argv[2]) + "/supersonic/doc/_bulk"
                    + "' -H \"Content-Type: application/json\" -d "
                    + params)
                total = total + count
                count = 0
                params = ""


def send_request(request):
    os.system(request)
    print("--- %s seconds ---" % (time.time() - start_time)) # calculate program run time

readfile()
p = Pool(40) #number of process
p.map(send_request, requests) # requests: array of params
