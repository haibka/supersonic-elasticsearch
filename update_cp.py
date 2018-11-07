# update customer_pay
import os
memo = "dekiru"
customer_pay_id = "52866603"
account_number = "12345678"
azukari_status_id = "23"
np_transaction_id = None
receipts_date = None
transfer_name = "hai"
shop_code = None

def check_output(value):
    if(value is None):
        return "null"
    else:
        return "\\\"" + value + "\\\""

        
os.system("curl -XPOST 'https://search-supersonic-mvgfqpixln7qd3ldpqpslpenra.ap-northeast-1.es.amazonaws.com/supersonic/customer_pay/"
    + customer_pay_id + "/_update'"
    + " -H \"Content-Type: application/json\" -d \"{"
    + "\\\"script\\\":{"
        + "\\\"source\\\": \\\"ctx._source.account_number = (params.account_number != null"
        + ") ? params.account_number : ctx._source.account_number; ctx._source.azukari_status_id = ("
        + "params.azukari_status_id != null) ? params.azukari_status_id: ctx._source.azukari_status_id; ctx._source.np_transaction_id = ("
        + "params.np_transaction_id != null) ? params.np_transaction_id : ctx._source.np_transaction_id; ctx._source.receipts_date = ("
        + "params.receipts_date != null) ? params.receipts_date : ctx._source.receipts_date; ctx._source.transfer_name = ("
        + "params.transfer_name != null) ? params.transfer_name : ctx._source.transfer_name; ctx._source.shop_code = ("
        + "params.shop_code != null) ? params.shop_code : ctx._source.shop_code;\\\","
        + "\\\"lang\\\": \\\"painless\\\","
        + "\\\"params\\\" : {"
            + "\\\"account_number\\\":" + check_output(account_number) + ","
            + "\\\"azukari_status_id\\\":" + check_output(azukari_status_id) + ","
            + "\\\"np_transaction_id\\\":" + check_output(np_transaction_id) + ","
            + "\\\"receipts_date\\\":" + check_output(receipts_date) + ","
            + "\\\"transfer_name\\\":" + check_output(transfer_name) + ","
            + "\\\"shop_code\\\":" + check_output(shop_code)
        + "}"
    + "}"
    + "}\"")
