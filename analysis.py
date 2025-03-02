import pandas as pd
import json

# Load and normalize users data
users_df = pd.read_json('users.json', lines=True)
users_df = pd.json_normalize(users_df.to_dict(orient='records'), sep='_')
# print(users_df)

# Load and normalize brands data
brands_df = pd.read_json('brands.json', lines=True)
brands_df = pd.json_normalize(brands_df.to_dict(orient='records'), sep='_')

# Load and normalize receipt data
receipts_df = pd.read_json('receipts.json', lines=True)
receipts_df = pd.json_normalize(receipts_df.to_dict(orient='records'), sep='_')

# Expand 'rewardsReceiptItemList' into separate rows
receipt_items_df = receipts_df.explode('rewardsReceiptItemList')
receipt_items_df = pd.json_normalize(receipt_items_df['rewardsReceiptItemList'])

# Display the data
print("Users Data:")
print(users_df.head())

print("\nbrands Data:")
print(brands_df.head())

print("\nReceipts Data:")
print(receipts_df.head())

print("\nReceipt Items Data:")
print(receipt_items_df.head())