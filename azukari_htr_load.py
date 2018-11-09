# add more azukari_history to customer_pay

import csv
import os
fname = "azukari_history.csv"
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

        print("insert id: " + id + "\n")
        os.system("curl -XPOST 'https://search-super-sonic-3rbymj6u6ymc5squcbhge3vdla.ap-northeast-1.es.amazonaws.com/supersonic/customer_pay/"
            + customer_pay_id + "/_update' -H \"Content-Type: application/json\" -d \"{  \\\"script\\\" : {"
            + "\\\"inline\\\": \\\"ctx._source.azukari_histories.add(params.new_azukari_htr)\\\", \\\"params\\\" : { \\\"new_azukari_htr\\\" : { "
            + "\\\"id\\\": \\\"" + id + "\\\","
          	+ "\\\"memo\\\" :\\\"" +  memo + "\\\""
            + "}"
        	+ "}"
            + "}"
            + "}\"")
