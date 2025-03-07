import os
import json
import csv
import pandas as pd
import mysql.connector
from datetime import datetime

#abs path of json dataset
BASE_DIR = 'data_folder'

#create folder sql_queries to store the csv files after executing json to csv conversion fucntion.
OUTPUT_DIR = os.path.join(os.getcwd(), 'SQL_queries')
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Converting the json to csv for uplaoing csv to mysql workbench.
class MongoDBProcessor:
    def __init__(self, users_file, brands_file, receipts_file):
        self.users_file = users_file
        self.brands_file = brands_file
        self.receipts_file = receipts_file

    @staticmethod
    def load_mongo_json(file_path):
        #Load MongoDB JSON data and return it as a list of dictionaries.
        with open(file_path, 'r', encoding='utf-8') as file:
            return [json.loads(line) for line in file]

    def write_to_csv(self, file_name, data, fieldnames):
        #Write data to a CSV file inside the SQL_queries folder.
        output_path = os.path.join(OUTPUT_DIR, file_name)
        with open(output_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)

    @staticmethod
    def convert_mongo_date(mongo_date):
        #Convert MongoDB timestamp format to SQL-friendly format.
        if mongo_date and isinstance(mongo_date, dict) and "$date" in mongo_date:
            return datetime.utcfromtimestamp(mongo_date["$date"] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        return None

    def process_users_data(self, users_data):
        #Process and return a list of user data.
        users = [{
            "user_id": user["_id"]["$oid"],
            "active": user.get("active", None),
            "created_date": self.convert_mongo_date(user.get("createdDate", None)),
            "last_login": self.convert_mongo_date(user.get("lastLogin", None)),
            "role": user.get("role", None),
            "signup_source": user.get("signUpSource", None),
            "state": user.get("state", None)
        } for user in users_data]
        return users

    def process_brands_data(self, brands_data):
        #Process and return brand data and CPGs.
        brands = []
        cpgs = {}
        for brand in brands_data:
            brand_data = {
                "brand_id": brand["_id"]["$oid"],
                "barcode": brand.get("barcode", None),
                "brand_code": brand.get("brandCode", None),
                "category": brand.get("category", None),
                "category_code": brand.get("categoryCode", None),
                "name": brand.get("name", None),
                "top_brand": brand.get("topBrand", None),
                "cpg_id": brand.get("cpg", {}).get("$id", {}).get("$oid", None)
            }
            brands.append(brand_data)
            cpg_id = brand_data["cpg_id"]
            if cpg_id:
                cpgs[cpg_id] = {
                    "cpg_id": cpg_id,
                    "name": brand.get("cpg", {}).get("$ref", None)
                }
        return brands, cpgs

    def process_receipts_data(self, receipts_data):
        #Process and return receipt data and receipt items.
        receipts = []
        receipt_items = []
        for receipt in receipts_data:
            receipt_data = {
                "r_id": receipt["_id"]["$oid"],
                "bonus_points_earned": receipt.get("bonusPointsEarned", None),
                "bonus_points_earned_reason": receipt.get("bonusPointsEarnedReason", None),
                "create_date": self.convert_mongo_date(receipt.get("createDate", None)),
                "date_scanned": self.convert_mongo_date(receipt.get("dateScanned", None)),
                "finished_date": self.convert_mongo_date(receipt.get("finishedDate", None)),
                "modify_date": self.convert_mongo_date(receipt.get("modifyDate", None)),
                "points_awarded_date": self.convert_mongo_date(receipt.get("pointsAwardedDate", None)),
                "points_earned": receipt.get("pointsEarned", None),
                "purchase_date": self.convert_mongo_date(receipt.get("purchaseDate", None)),
                "purchased_item_count": receipt.get("purchasedItemCount", None),
                "rewards_receipt_status": receipt.get("rewardsReceiptStatus", None),
                "total_spent": receipt.get("totalSpent", None),
                "user_id": receipt.get("userId", None)
            }
            receipts.append(receipt_data)

            rewardsReceiptItemList = receipt.get("rewardsReceiptItemList", [])
            for item in rewardsReceiptItemList:
                receipt_item = {
                    "receipt_id": receipt["_id"]["$oid"],
                    "barcode": item.get("barcode", None),
                    "description": item.get("description", None),
                    "final_price": item.get("finalPrice", None),
                    "item_price": item.get("itemPrice", None),
                    "quantity_purchased": item.get("quantityPurchased", None)
                }
                receipt_items.append(receipt_item)
        return receipts, receipt_items

    def segregate_data(self):
        base_path = os.path.abspath(BASE_DIR)
        users_data = self.load_mongo_json(os.path.join(base_path, self.users_file))
        brands_data = self.load_mongo_json(os.path.join(base_path, self.brands_file))
        receipts_data = self.load_mongo_json(os.path.join(base_path, self.receipts_file))

        users = self.process_users_data(users_data)
        brands, cpgs = self.process_brands_data(brands_data)
        receipts, receipt_items = self.process_receipts_data(receipts_data)

        self.write_to_csv("users_data.csv", users, ["user_id", "active", "created_date", "last_login", "role", "signup_source", "state"])
        self.write_to_csv("brands_data.csv", brands, ["brand_id", "barcode", "brand_code", "category", "category_code", "name", "top_brand", "cpg_id"])
        self.write_to_csv("cpgs_data.csv", list(cpgs.values()), ["cpg_id", "name"])
        self.write_to_csv("receipts_data.csv", receipts, ["r_id", "bonus_points_earned", "bonus_points_earned_reason", "create_date", "date_scanned", "finished_date", "modify_date", "points_awarded_date", "points_earned", "purchase_date", "purchased_item_count", "rewards_receipt_status", "total_spent", "user_id"])
        self.write_to_csv("receipt_items_data.csv", receipt_items, ["receipt_id", "barcode", "description", "final_price", "item_price", "quantity_purchased"])

        print("CSV files generated in SQL_queries folder.")

# Uploading generated csv to MySQL workbench
class MySQLUploader:
    def __init__(self, db_config):
        self.db_config = db_config
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.db_config)
            self.cursor = self.connection.cursor()
            print("Connected to MySQL database")
        except mysql.connector.Error as err:
            print(f"Error connecting to database: {err}")

    def disconnect(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
            print("Disconnected from MySQL database")

    def upload_data(self, table_name, csv_file, columns, boolean_columns=None):
        file_path = os.path.abspath(csv_file)
        df = pd.read_csv(file_path)

        # Handle NaN values by replacing them with None.
        df = df.applymap(lambda x: None if pd.isna(x) else x)

        # Convert boolean columns to integer 0/1 if specified
        if boolean_columns:
            for col in boolean_columns:
                if col in df.columns:
                    df[col] = df[col].map({'True': 1, 'False': 0, None: None})
                else:
                    print(f"Warning: Column '{col}' not found in CSV for {table_name}")

        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"

        try:
            for _, row in df.iterrows():
                if len(row) != len(columns):
                    print(f"Skipping row due to column mismatch: {row}")
                    continue
                self.cursor.execute(query, tuple(row))
            self.connection.commit()
            print(f"Data inserted successfully into {table_name}!")
        except mysql.connector.Error as err:
            print(f"Error inserting data into {table_name}: {err}")

    

# Evaluate Data Quality Issues:
def load_and_normalize(file_name, sep='_'):
    file_path = os.path.join(BASE_DIR, file_name)
    df = pd.read_json(file_path, lines=True)
    df = pd.json_normalize(df.to_dict(orient='records'), sep=sep)
    return df

def check_data_quality(df, name):
    print(f"\n{name} Data Quality Check:")
    print(df.info())
    print(f"\n Evaluating Missing values:\n", df.isnull().sum())            #Find missing values
    print(f"\n Evaluating Duplicate values:", df.duplicated().sum())        #Find duplicates

#check for any inconsistencies in the dataset.
def check_date_consistency(df, date_columns, name):
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    invalid_dates = df[df[date_columns].isna().any(axis=1)]
    
    if invalid_dates.empty:
        print(f"\nNo inconsistent date formats found in {name}")
    else:
        print(f"\nInconsistent date formats found in {name}")
        print(invalid_dates)

# Main Execution
def main():
    # Step 1: Convert JSON to CSV
    processor = MongoDBProcessor('users.json', 'brands.json', 'receipts.json')
    processor.segregate_data()

    # Step 2: Upload CSV to MySQL
    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'Vishruti@123',
        'database': 'fetch_challenge',
    }

    uploader = MySQLUploader(db_config)
    uploader.connect()

    tables = {
        'users_data': ['user_id', 'active', 'created_date', 'last_login', 'role', 'signup_source', 'state'],
        'brands_data': ['brand_id', 'barcode', 'brand_code', 'category', 'category_code', 'name', 'top_brand', 'cpg_id'],
        'cpgs_data': ['cpg_id', 'name'],
        'receipts_data': ['r_id','bonus_points_earned','bonus_points_earned_reason','create_date','date_scanned','finished_date','modify_date','points_awarded_date','points_earned','purchase_date','purchased_item_count','rewards_receipt_status','total_spent','user_id'],
        'receipt_items_data': ['receipt_id', 'barcode', 'description', 'final_price', 'item_price', 'quantity_purchased'],
    }

    boolean_columns_map = {
        'brands_data': ['top_brand'],
        'receipt_items_data': ['needs_fetch_review', 'user_flagged_new_item']
    }

    for table_name, columns in tables.items():
        csv_file = f'./SQL_queries/{table_name}.csv'
        boolean_columns = boolean_columns_map.get(table_name, None)
        uploader.upload_data(table_name, csv_file, columns, boolean_columns)

    uploader.disconnect()

    # Step 3: Evaluate Data Quality
    users_df = load_and_normalize('users.json')
    brands_df = load_and_normalize('brands.json')
    receipts_df = load_and_normalize('receipts.json')

    receipt_items_df = receipts_df.explode('rewardsReceiptItemList')
    receipt_items_df = pd.json_normalize(receipt_items_df['rewardsReceiptItemList'])

    check_data_quality(users_df, 'Users')
    check_data_quality(brands_df, 'Brands')
    check_data_quality(receipt_items_df, 'Receipts')

    check_date_consistency(users_df, ['createdDate_$date'], 'Users dataset')
    check_date_consistency(receipts_df, ['dateScanned_$date', 'createDate_$date'], 'Receipts dataset')

if __name__ == "__main__":
    main()
