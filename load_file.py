import csv
import os
fname = "LOAD00000001.csv"
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
        transfer_name = param.split(",")[22]
        azukari_status_id = param.split(",")[37]
        print("insert id: " + id + "\n")
        # os.system("curl -XPUT 'https://search-supersonic-mvgfqpixln7qd3ldpqpslpenra.ap-northeast-1.es.amazonaws.com/supersonic/customer_pay/"
        #     + id + "' -H \"Content-Type: application/json\" -d \"{ \\\"id\\\": \\\"" + id + "\\\","
        #     + "\\\"account_number\\\": \\\"" + account_number + "\\\","
        #     + "\\\"receipts_date\\\": " + receipts_date + ","
        #     + "\\\"receipts_amount\\\": " + receipts_amount + ","
        #     + "\\\"shop_code\\\": \\\"" + shop_code + "\\\","
        #     + "\\\"np_transacton_id\\\": \\\"" + np_transacton_id + "\\\","
        #     + "\\\"transfer_name\\\": \\\"" + transfer_name + "\\\","
        #     + "\\\"azukari_status_id\\\": " + azukari_status_id + ","
        #     + "\\\"azukari_histories\\\": []"
        #     + "}\"")


        os.system("curl -XPUT 'https://search-supersonic-mvgfqpixln7qd3ldpqpslpenra.ap-northeast-1.es.amazonaws.com/supersonic/customer_pay/"
            + id + "' -H \"Content-Type: application/json\" -d \"{ \\\"id\\\": \\\"" + id + "\\\","
            + "\\\"account_number\\\": \\\"" + account_number + "\\\","
            + "\\\"receipts_date\\\": \\\"" + receipts_date + "\\\","
            + "\\\"receipts_amount\\\": \\\"" + receipts_amount + "\\\","
            + "\\\"shop_code\\\": \\\"" + shop_code + "\\\","
            + "\\\"np_transaction_id\\\": \\\"" + np_transacton_id + "\\\","
            + "\\\"transfer_name\\\": \\\"" + transfer_name + "\\\","
            + "\\\"azukari_status_id\\\": \\\"" + azukari_status_id + "\\\","
            + "\\\"azukari_histories\\\": []"
            + "}\"")












# id = "a"
# account_number = "a"
# receipts_date = "a"
# receipts_amount = "a"
# shop_code = "a"
# np_transacton_id = "a"
# transfer_name = "a"
# azukari_status_id = "a"
# print("curl -XPUT 'https://search-supersonic-mvgfqpixln7qd3ldpqpslpenra.ap-northeast-1.es.amazonaws.com/supersonic/customer-pay/"
#     + id + "' -H \"Content-Type: application/json\" -d \"{ \\\"id\\\": \\\"" + id + "\\\","
#     + "\\\"account_number\\\": \\\"" + account_number + "\\\","
#     + "\\\"receipts_date\\\": \\\"" + receipts_date + "\\\","
#     + "\\\"receipts_amount\\\": \\\"" + receipts_amount + "\\\","
#     + "\\\"shop_code\\\": \\\"" + shop_code + "\\\","
#     + "\\\"np_transacton_id\\\": \\\"" + np_transacton_id + "\\\","
#     + "\\\"transfer_name\\\": \\\"" + transfer_name + "\\\","
#     + "\\\"azukari_status_id\\\": \\\"" + azukari_status_id + "\\\""
#     + "}\"")
