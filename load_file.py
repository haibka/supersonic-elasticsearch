import csv
import os
import time
from multiprocessing import Pool
start_time = time.time()
requests = []

def readfile():
    fname = "LOAD00000001.csv" #file data of customer_pay
    params = "" #param request to elastic search server
    total = 0 #record of number
    count = 0
    host = "https://search-supersonic-mvgfqpixln7qd3ldpqpslpenra.ap-northeast-1.es.amazonaws.com/supersonic/customer_pay/_bulk"
    with open(fname, 'rb') as csvfile:
        spamreader = csv.reader(csvfile)
        pool=Pool()
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
            params = params + "\"{ \\\"index\\\" : {\\\"_id\\\" : \\\"" + id + "\\\"} }\n\"" \
            + "\"{ \\\"id\\\": \\\"" + id + "\\\"," \
            + "\\\"account_number\\\": \\\"" + account_number.strip() + "\\\"," \
            + "\\\"receipts_date\\\": \\\"" + receipts_date.strip() + "\\\"," \
            + "\\\"receipts_amount\\\": \\\"" + receipts_amount.strip() + "\\\"," \
            + "\\\"shop_code\\\": \\\"" + shop_code.strip() + "\\\"," \
            + "\\\"np_transaction_id\\\": \\\"" + np_transaction_id.strip() + "\\\"," \
            + "\\\"ir_flg\\\": \\\"" + ir_flg.strip() + "\\\"," \
            + "\\\"transfer_name\\\": \\\"" + transfer_name.strip() + "\\\"," \
            + "\\\"bank_code\\\": \\\"" + bank_code.strip() + "\\\"," \
            + "\\\"tantocd\\\": \\\"" + tantocd.strip() + "\\\"," \
            + "\\\"action_id\\\": \\\"" + action_id.strip() + "\\\"," \
            + "\\\"azukari_status_id\\\": \\\"" + azukari_status_id.strip() + "\\\"," \
            + "\\\"furikae_status_id\\\": \\\"" + furikae_status_id.strip() + "\\\"," \
            + "\\\"azukari_histories\\\": []" \
            + "}\n\""
            count = count + 1
            if (count == 570):
                requests.append("curl -XPOST '"
                    + host
                    + "' -H \"Content-Type: application/json\" -d "
                    + params)
                total = total + count
                count = 0
                params = ""


        if(count > 0):
            requests.append("curl -XPOST '"
                + host
                + "' -H \"Content-Type: application/json\" -d "
                + params)
            total = total + count


def send_request(request):
    os.system(request)
    print("--- %s seconds ---" % (time.time() - start_time)) # calculate program run time

readfile()
p = Pool(20) #number of process
p.map(send_request, requests) # requests: array of params
