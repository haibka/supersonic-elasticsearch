# command run: python load_azukari_history.py ./azukari_history elasticsearch_url
# add more azukari_history to customer_pay
import csv
import os
import time
import sys
from multiprocessing import Pool

def custom_memo(memo):
    strings = memo.split("\n")
    result = ""
    for a in strings:
        result = result + a + "\\\n"
    return result

start_time = time.time()
requests = []
def readfile():
    for file in os.listdir(str(sys.argv[1])):
        fname = str(sys.argv[1]) + "/" + file
        params = ""
        count = 0
        total = 0
        with open(fname, 'rb') as csvfile:
            spamreader = csv.reader(csvfile)
            for row in spamreader:
                param = ', '.join(row)
                id = param.split(",")[0]
                customer_pay_id = param.split(",")[1].strip()
                tantocd = param.split(",")[2].strip()
                ir_flg = param.split(",")[3].strip()
                action_id = param.split(",")[4].strip()
                furikae_status_id = param.split(",")[5].strip()
                azukari_status_id = param.split(",")[6].strip()
                memo = param.split(",")[7].strip()
                r_id = param.split(",")[11].strip()
                r_ymd = param.split(",")[12].strip()

                params = params + "\"{ \\\"update\\\" : {\\\"_id\\\" : \\\"" + customer_pay_id + "\\\", \\\"_retry_on_conflict\\\" : 3} }\n\"" \
                    + "\"{  \\\"script_upsert\\\" : true, \\\"script\\\" :{" \
                        + "\\\"source\\\": \\\"ctx._source.azukari_histories.add(params.new_azukari_htr)\\\"," \
                        + "\\\"params\\\" : { " \
                            + "\\\"new_azukari_htr\\\" : { " \
                            + "\\\"id\\\": \\\"" + id + "\\\"," \
                            + "\\\"tantocd\\\": \\\"" + tantocd + "\\\"," \
                            + "\\\"ir_flg\\\" :\\\"" +  ir_flg + "\\\"," \
                            + "\\\"action_id\\\" :\\\"" +  action_id + "\\\"," \
                            + "\\\"furikae_status_id\\\" :\\\"" +  furikae_status_id + "\\\"," \
                            + "\\\"azukari_status_id\\\" :\\\"" +  azukari_status_id + "\\\"," \
                            + "\\\"memo\\\" :\\\"" +  custom_memo(memo) + "\\\"," \
                            + "\\\"r_id\\\" :\\\"" +  r_id + "\\\"," \
                            + "\\\"r_ymd\\\" :\\\"" +  r_ymd + "\\\"" \
                            + "}" \
                        + "}" \
                    + "}" \
                    + "}\n\""
                count = count + 1
                if (count == 570):
                    requests.append("curl -XPOST '"
                        + str(sys.argv[2]) + "/supersonic/doc/_bulk"
                        + "' -H \"Content-Type: application/json\" -d "
                        + params )
                    total = total + count
                    count = 0
                    params = ""
                    print("Add params azukari_history")

            if(count > 0):
                requests.append("curl -XPOST '"
                    + str(sys.argv[2]) + "/supersonic/doc/_bulk"
                    + "' -H \"Content-Type: application/json\" -d "
                    + params )
                total = total + count
                count = 0
                params = ""
                # print("Number azukari_history record: " + str(total))

def send_request(request):
    os.system(request)
    print("--- %s seconds ---" % (time.time() - start_time)) # calculate program run time

readfile()
p = Pool(40) #number of process
p.map(send_request, requests) # requests: array of params
