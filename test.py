#!/usr/bin/env python
# -*- coding: utf-8 -*-
def custom_memo(memo):
    strings = memo.splitlines()
    result = ""
    for a in strings:
        result = result + a + "\n"
    return result
param = "0"
status = "0"
id = "0"
tantocd = "0"
ir_flg = "0"
action_id = "0"
furikae_status_id = "0"
azukari_status_id = "0"
memo = "取引ID：akt001140018061400500\n" \
+ "他取引ID：\n" \
+ "ルールエンジン結果：03\n"
r_id = "0"
r_ymd = "0"
customer_pay_id = "0"

print("\"{ \\\"update\\\" : {\\\"_id\\\" : \\\"" + customer_pay_id.strip() + "\\\",\\\"_retry_on_conflict\\\" : 3} }\n\"" \
    + "\"{\\\"scripted_upsert\\\": true,\\\"script\\\" : {\\\"source\\\": \\\"if (ctx._source.azukari_histories==null) ctx._source.azukari_histories = [];" \
    + "ctx._source.azukari_histories.add(params.new)\\\",\\\"params\\\" : {\\\"new\\\" : {" \
    + "\\\"id\\\": \\\"" + id.strip() + "\\\"," \
    + "\\\"tantocd\\\": \\\"" + tantocd.strip() + "\\\"," \
    + "\\\"ir_flg\\\": \\\"" + ir_flg.strip() + "\\\"," \
    + "\\\"action_id\\\":\\\"" + action_id.strip() + "\\\"," \
    + "\\\"furikae_status_id\\\": \\\"" + furikae_status_id.strip() + "\\\"," \
    + "\\\"azukari_status_id\\\": \\\"" + azukari_status_id.strip() + "\\\"," \
    + "\\\"memo\\\": \\\"" + memo.strip() + "\\\"," \
    + "\\\"r_id\\\": \\\"" + r_id.strip() + "\\\"," \
    + "\\\"r_ymd\\\": \\\"" + r_ymd.strip() + "\\\"}}}," \
    + "\\\"upsert\\\" : {\\\"azukari_histories\\\" : []}}\n\"")
print("{ \"update\" : {\"_id\" : \"0\",\"_retry_on_conflict\" : 3} }\n" +
"{\"scripted_upsert\": true,\"script\" : {\"source\": \"if (ctx._source.azukari_histories==null) ctx._source.azukari_histories = [];ctx._source.azukari_histories.add(params.new)\",\"params\" : {\"new\" : {\"id\": \"0\",\"tantocd\": \"0\",\"ir_flg\": \"0\",\"action_id\":\"0\",\"furikae_status_id\": \"0\",\"azukari_status_id\": \"0\",\"memo\": \"0\",\"r_id\": \"0\",\"r_ymd\": \"0\"}}},\"upsert\" : {\"azukari_histories\" : []}}")
