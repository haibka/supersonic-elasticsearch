#!/usr/bin/env python
# -*- coding: utf-8 -*-
def custom_memo(memo):
    strings = memo.splitlines()
    result = ""
    for a in strings:
        result = result + a + "|"
    return result
id = "0"
customer_pay_id = "0"
tantocd = "0"
ir_flg = "0"
action_id = "0"
furikae_status_id = "0"
azukari_status_id = "0"
memo = "0"
r_id = "0"
r_ymd = "0"

print("\"{\\\"update\\\": {\\\"_id\\\": \\\"" + id.strip() + "\\\", \\\"_retry_on_conflict\\\" : 3}}\n" \
    + "\"{\\\"script\\\" : {\\\"source\\\": \\\"ctx._source.remove(\\\\\\\"otoiawasebango\\\\\\\"); " \
    + "ctx._source.remove(\\\\\\\"receipts_date\\\\\\\"); ctx._source.remove(\\\\\\\"transfer_name\\\\\\\"); " \
    + "ctx._source.remove(\\\\\\\"azukari_status_id\\\\\\\"); ctx._source.remove(\\\\\\\"receipts_amount\\\\\\\"); " \
    + "ctx._source.remove(\\\\\\\"account_number\\\\\\\"); ctx._source.remove(\\\\\\\"ir_flg\\\\\\\"); " \
    + "ctx._source.remove(\\\\\\\"bank_code\\\\\\\"); ctx._source.remove(\\\\\\\"branch_code\\\\\\\"); " \
    + "ctx._source.remove(\\\\\\\"tantocd\\\\\\\"); ctx._source.remove(\\\\\\\"action_id\\\\\\\"); " \
    + "ctx._source.remove(\\\\\\\"furikae_status_id\\\\\\\"); ctx._source.remove(\\\\\\\"shop_code\\\\\\\"); " \
    + "ctx._source.remove(\\\\\\\"np_transaction_id\\\\\\\");\\\"}}\n\"")

print("{\"update\": {\"_id\": \"0\", \"_retry_on_conflict\" : 3}}"+
"{\"script\" : {\"source\": \"ctx._source.remove(\\\"otoiawasebango\\\"); ctx._source.remove(\\\"receipts_date\\\"); ctx._source.remove(\\\"transfer_name\\\"); ctx._source.remove(\\\"azukari_status_id\\\"); ctx._source.remove(\\\"receipts_amount\\\"); ctx._source.remove(\\\"account_number\\\"); ctx._source.remove(\\\"ir_flg\\\"); ctx._source.remove(\\\"bank_code\\\"); ctx._source.remove(\\\"branch_code\\\"); ctx._source.remove(\\\"tantocd\\\"); ctx._source.remove(\\\"action_id\\\"); ctx._source.remove(\\\"furikae_status_id\\\"); ctx._source.remove(\\\"shop_code\\\"); ctx._source.remove(\\\"np_transaction_id\\\");\"}}")
