_id: shopcode + np_transaction_id
seikyu: {
    kaisyacd
    seqno
    gyono
    customer_name
    seikyugaku
    otoriawasebango(=shopcode + np_transaction_id)
    customer_pay {
        id
        account_number
        receipts_date
        receipts_amount
        ir_flg
        transfer_name
        bank_code
        branch_code
        tantocd
        action_id
        furikae_status_id
        azukari_status_id
        azukari_histories {
            id
            ir_flg
            action_id
            furikae_status_id
            azukari_status_id
            r_ymd
            r_id
            tantocd
            memo
        }
    }
}
