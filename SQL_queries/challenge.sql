-- Query-1
-- What is this query for. 


SELECT 
    receipt.rewards_receipt_status,
    SUM(items.quantity_purchased) AS total_items_purchased
FROM fetch_challenge.receipts_data receipt
JOIN fetch_challenge.receipt_items_data items ON receipt.r_id = items.receipt_id
WHERE receipt.rewards_receipt_status IN ('Accepted', 'Rejected')
GROUP BY receipt.rewards_receipt_status;




-- Query-2
