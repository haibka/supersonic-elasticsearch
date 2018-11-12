# add more azukari_history to customer_pay
import csv
import os
import time
from multiprocessing import Pool

start_time = time.time()
requests = []
def readfile():
    fname = "azukari_history.csv"
    host = "https://search-supersonic-mvgfqpixln7qd3ldpqpslpenra.ap-northeast-1.es.amazonaws.com/supersonic/customer_pay/_bulk"
    params = ""
    count = 0
    total = 0
    with open(fname, 'rb') as csvfile:
        spamreader = csv.reader(csvfile)
        for row in spamreader:
            param = ', '.join(row)
            id = param.split(",")[0]
            customer_pay_id = param.split(",")[1].strip()
            ir_flg = param.split(",")[3].strip()
            action_id = param.split(",")[4].strip()
            furikae_status_id = param.split(",")[5].strip()
            azukari_status_id = param.split(",")[6].strip()
            memo = param.split(",")[7].strip()
            r_ymd = param.split(",")[12].strip()

            params = params + "\"{ \\\"update\\\" : {\\\"_id\\\" : \\\"" + customer_pay_id + "\\\", \\\"_retry_on_conflict\\\" : 3} }\n\"" \
            + "\"{  \\\"script\\\" : {" \
            + "\\\"inline\\\": \\\"ctx._source.azukari_histories.add(params.new_azukari_htr)\\\", \\\"lang\\\" : \\\"painless\\\", " \
            + "\\\"params\\\" : { \\\"new_azukari_htr\\\" : { " \
            + "\\\"id\\\": \\\"" + id + "\\\"," \
            + "\\\"ir_flg\\\" :\\\"" +  ir_flg + "\\\"," \
            + "\\\"action_id\\\" :\\\"" +  action_id + "\\\"," \
            + "\\\"furikae_status_id\\\" :\\\"" +  furikae_status_id + "\\\"," \
            + "\\\"azukari_status_id\\\" :\\\"" +  azukari_status_id + "\\\"," \
            + "\\\"memo\\\" :\\\"" +  memo + "\\\"," \
            + "\\\"r_ymd\\\" :\\\"" +  r_ymd + "\\\"" \
            + "}" \
            + "}" \
            + "}" \
            + "}\n\""
            count = count + 1
            if (count == 570):
                requests.append("curl -XPOST '"
                    + host
                    + "' -H \"Content-Type: application/json\" -d "
                    + params )
                total = total + count
                count = 0
                params = ""
                # print("Number azukari_history record: " + str(total))
    if(count > 0):
        request.append("curl -XPOST '"
            + host
            + "' -H \"Content-Type: application/json\" -d "
            + params )
        total = total + count
        # print("Number azukari_history record: " + str(total))

def send_request(request):
    os.system(request)
    print("--- %s seconds ---" % (time.time() - start_time)) # calculate program run time

readfile()
p = Pool(20) #number of process
p.map(send_request, requests) # requests: array of params
