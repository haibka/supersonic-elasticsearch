import json
import os

count = 0
def check_output(value):
    if(value is None):
        return "null"
    else:
        return "\\\"" + value + "\\\""

def lambda_handler(event, context):
    global count
    for record in event['Records']:
        body = record["body"]
        messageAttributes = record["messageAttributes"]
        print(str(body))
        status = messageAttributes["SYS_CHANGE_OPERATION"]["stringValue"]
        print("\nSYS_CHANGE_OPERATION: " + status)
        if(str(body) == "CUSTOMER_PAY"):
            count = count + 1
            print("count message: " + str(count))
            id = messageAttributes["ID"]["stringValue"].strip()
            print("ID: " + id)
            if(status != "D"):
                account_name = messageAttributes["ACCOUNT_NUMBER"]["stringValue"]
                print("\nACCOUNT_NUMBER: " + account_number)
                receipts_date = messageAttributes["RECEIPTS_DATE"]["stringValue"]
                print("\nRECEIPTS_DATE: " + receipts_date)
                tantocd = messageAttributes["TANTOCD"]["stringValue"]
                print("\nTANTOCD: " + tantocd)
                bank_code = messageAttributes["BANK_CODE"]["stringValue"]
                print("\nBANK_CODE: " + bank_code)
                branch_code = messageAttributes["BRANCH_CODE"]["stringValue"]
                print("\nBRANCH_CODE: " + branch_code)
                azukari_status_id = messageAttributes["AZUKARI_STATUS_ID"]["stringValue"]
                print("\nAZUKARI_STATUS_ID: " + azukari_status_id)
                receipts_amount = messageAttributes["RECEIPTS_AMOUNT"]["stringValue"]
                print("\nRECEIPTS_AMOUNT: " + receipts_amount)
        if(str(body) == "AZUKARI_HISTORY"):
            count = count + 1
            print("count message: " + str(count))
            id = messageAttributes["ID"]["stringValue"].strip()
            print("ID: " + id)
            if(status != "D"):
                ir_flg = messageAttributes["IR_FLG"]["stringValue"]
                print("\nIR_FLG: " + ir_flg)
                action_id = messageAttributes["ACTION_ID"]["stringValue"]
                print("\nACTION_ID: " + action_id)
                furikae_status_id = messageAttributes["FURIKAE_STATUS_ID"]["stringValue"]
                print("\nFURIKAE_STATUS_ID: " + furikae_status_id)
                azukari_status_id = messageAttributes["AZUKARI_STATUS_ID"]["stringValue"]
                print("\nAZUKARI_STATUS_ID: " + azukari_status_id)
                r_ymd = messageAttributes["R_YMD"]["stringValue"]
                print("\nR_YMD: " + r_ymd)
                memo = messageAttributes["MEMO"]["stringValue"]
                print("\nMEMO: " + memo)
                r_id = messageAttributes["R_ID"]["stringValue"]
                print("\nR_ID: " + r_id)
                tantocd = messageAttributes["TANTOCD"]["stringValue"]
                print("\nTANTOCD: " + tantocd)
                customer_pay_id = messageAttributes["CUSTOMER_PAY_ID"]["stringValue"]
                print("\nCUSTOMER_PAY_ID: " + customer_pay_id)
        if(str(body) == "SEIKYU"):
            count = count + 1
            print("count message: " + str(count))
            kaisyacd = messageAttributes["KAISYACD"]["stringValue"].strip()
            print("\nKAISYACD: " + kaisyacd)
            seqno = messageAttributes["SEQNO"]["stringValue"].strip()
            print("\nSEQNO: " + seqno)
            gyono = messageAttributes["GYONO"]["stringValue"].strip()
            print("\nGYONO: " + gyono)
            if(status != "D"):
                customer_name = messageAttributes["CUSTOMER_NAME"]["stringValue"]
                print("\nCUSTOMER_NAME: " + customer_name)
                seikyugaku = messageAttributes["SEIKYUGAKU"]["stringValue"].strip()
                print("SEIKYUGAKU: " + seikyugaku)
        if(str(body) == "NYUKIN"):
            count = count + 1
            print("count message: " + str(count))
            kaisyacd = messageAttributes["KAISYACD"]["stringValue"].strip()
            print("\nKAISYACD: " + kaisyacd)
            seqno = messageAttributes["SEQNO"]["stringValue"].strip()
            print("\nSEQNO: " + seqno)
            gyono = messageAttributes["GYONO"]["stringValue"].strip()
            print("\nGYONO: " + gyono)
            if(status != "D"):
                kingaku = messageAttributes["KINGAKU"]["stringValue"]
                print("\nKINGAKU: " + kingaku)
                authentification_cp_id = messageAttributes["AUTHENTIFICATION_CP_ID"]["stringValue"].strip()
                print("\nAUTHENTIFICATION_CP_ID: " + authentification_cp_id)

    # if(status != "D"):
    #     os.system("curl -XPOST 'https://search-supersonic-mvgfqpixln7qd3ldpqpslpenra.ap-northeast-1.es.amazonaws.com/demo/test/"
    #         + id + "'"
    #         + " -H \"Content-Type: application/json\" -d \"{"
    #                 + "\\\"id\\\":" + check_output(id).strip() + ","
    #                 + "\\\"name\\\":" + check_output(name).strip() + ","
    #                 + "\\\"age\\\":" + check_output(str(age)).strip()
    #         + "}\"")
    # else:
    #     os.system("curl -XDELETE 'https://search-supersonic-mvgfqpixln7qd3ldpqpslpenra.ap-northeast-1.es.amazonaws.com/demo/test/"
    #         + id + "'")
