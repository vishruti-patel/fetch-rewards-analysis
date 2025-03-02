# import pandas as pd
# import json

# # Load and normalize users data
# users_df = pd.read_json('users.json', lines=True)
# users_df = pd.json_normalize(users_df.to_dict(orient='records'), sep='_')
# # print(users_df)

# # Load and normalize brands data
# brands_df = pd.read_json('brands.json', lines=True)
# brands_df = pd.json_normalize(brands_df.to_dict(orient='records'), sep='_')

# # Load and normalize receipt data
# receipts_df = pd.read_json('receipts.json', lines=True)
# receipts_df = pd.json_normalize(receipts_df.to_dict(orient='records'), sep='_')

# # Expand 'rewardsReceiptItemList' into separate rows
# receipt_items_df = receipts_df.explode('rewardsReceiptItemList')
# receipt_items_df = pd.json_normalize(receipt_items_df['rewardsReceiptItemList'])

# # Display the data
# print("Users Data:")
# print(users_df.head())

# print("\nbrands Data:")
# print(brands_df.head())

# print("\nReceipts Data:")
# print(receipts_df.head())

# print("\nReceipt Items Data:")
# print(receipt_items_df.head())


import pandas as pd

def load_and_normalize_json(file_path, explode_column=None, sep='_', n_preview=5):
    try:
        df = pd.read_json(file_path, lines=True)
        normalized_df = pd.json_normalize(df.to_dict(orient='records'), sep=sep)
        
        if explode_column and explode_column in normalized_df.columns:
            # Explode the specified nested column into separate rows
            exploded_df = normalized_df.explode(explode_column)
            
            # Normalize the exploded column separately
            exploded_column_df = pd.json_normalize(
                exploded_df[explode_column].dropna(), sep=sep
            )
            
            # Combine the exploded data back with the original data
            exploded_combined_df = exploded_df.drop(columns=[explode_column]).reset_index(drop=True)
            exploded_column_df = exploded_column_df.reset_index(drop=True)
            
            # Concatenate both dataframes side-by-side
            final_df = pd.concat([exploded_combined_df, exploded_column_df], axis=1)
            
            print(f"\nPreview of {file_path} (Including Exploded Column '{explode_column}'):")
            print(final_df.head(n_preview))
            
            return final_df
        
        else:
            print(f"\nPreview of {file_path}:")
            print(normalized_df.head(n_preview))
            return normalized_df
    
    except Exception as e:
        print(f"Error loading {file_path}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error
    
# Load and normalize data from JSON files
# users_df = load_and_normalize_json('users.json')
# brands_df = load_and_normalize_json('brands.json')
receipts_df = load_and_normalize_json('receipts.json', explode_column='rewardsReceiptItemList') 