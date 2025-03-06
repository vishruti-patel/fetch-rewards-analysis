# fetch-rewards-analysis
Designing a structured data model, writing SQL queries to answer business questions and identifying data quality issues for Fetch Rewards.

## Overview
This is the small project challenhe where the primary objectives includes designing a structured relational data model to organize raw data effectively, assessing data quality by identifying issues such as missing values, duplicates, and inconsistent formats and developing SQL queries to answer business questions and validate data integrity. Additionally, a key component of the project was clear and effective communication with stakeholders, highlighting data insights, quality concerns, and requirements for further optimization. Overall, this project demonstrates technical skills in data engineering and analytics, as well as the ability to translate complex data findings into actionable insights and robust data solutions.

## Part one: Review json data and designig Relational Data Model.
This is the first part of the project which includes my understanding and assumptions of the provided datasets along with my approach behind designing a well structured Relational Data Model.

1. `Receipt Data Schema`: Includes the transaction details of the users along with the date/time information, Rewards/bonus points details and the receipt item list. The attribute rewardsReceiptItemList is a nested array which contains the details on items information such as quantity purchased, date, reviews ans also partner id specifics 

2. `Users Data Schema`: This dataset contains the information on user profile and activity data such as account status and login timestamps. Each user profile is linked to receipts data throgh user_id.

3. `Brands Data Schema`: This dataset stores information about product/item brands, including barcode, categorization, and brand attributes like the 'top brand' indicator. It connects with purchased items through barcodes or brand codes, enabling analysis of brand performance and consumer preferences. The attribute cpg in the Brands data is a nested array that has a reference to other collection. As there is no details provided for the nested attribute, I have proceeded with the assumption that the '$ref' likely refers to a collection of CPG(Consumer Packaged Goods) entity



### Relational Data Model overview:
The objective of the first part of this challenge is to design a structured Relational Data Model from the provided unstructured JSON data. I have used dbdiagram.io for designing the ER diagram. Below stated is my approach behind the data model.

1. **Analyzing the JSON schema**: Using the basic information provided, I analyzed the datasets and found that 2 of the attributes (rewardsReceiptItemList, cpg) is a nested array.
2. **Handling nested data**: The nested array RewardReceiptItemList in the receipts dataset contained detailed information about each purchased item, barcode, description, final_price, and other attributes. To normalize this data, I created a separate table receipt_items_data with a foreign key (receipt_id) linking it back to the Receipt table. Another attribute cpgs from Brands table is a nested array CPG containing $ref and $id, without additional details provided. Assuming the potential for future expansion of this data, I created another 'cpg' table with the cpg_id as a foreign key in the Brands table. This allows for potential integration with a more detailed Consumer Packaged Goods dataset.
3. **Identifying Key Entities and Relationships**: After handling the nested attributes with the above stated assumptions, I concluded main entities of this challenge: Users data, receipts data, receipt_items_data, brands data, cpgs_data along with the Primary Keys and Foreign Keys for each table as shown in the ER diagram. Moreover, I idenitfied one-to-many relationships among each entities as displayed in the screenshot below.
4. **Designing the ER diagram**: To illustrate the relational data model, I used dbdiagram.io. 


![alt text](<Review_json_data/Relational Data Model for Consumer Products.png>)


## Part two: SQL queries to answer questions from the business stakeholder.
For the second part of the challenge, I have utilized MySQL Workbench to write SQL queries that addressed specific business questions. I began by writing Python script to convert the provided JSON files into csv format for the ease of importing data into MySQL. This approach enables seamless data ingestion into MySQL Workbench. The conversion script is included in the SQL_queries/csv/json_to_csv.

The Python script automates the extraction and transformation of data from nested JSON files into a structured CSV format. Key steps of the scripts are:
- load_mongo_json function loads the JSON data from the MongoDB export and parse each line as a separate JSON object.
- The segregate_data function segregates data into five CSV files which will be generated after running the script sucessfully.
- convet_mongo_date function converts MongoDB $date timestaps to SQL format.

As the next step, I developed a second Python script to automate the upload of these CSV files into MySQL Workbench. I have used Pandas to read the CSV files and replace 'NaN' values with 'None' to handle 'NULL' values in MySQL. Using MySQL Connector, I established a connection to the database and executed INSERT queries to load data into the respective tables.

After susccefully importing the csv files to MYSQL workbench, I wrote following queries to answer the stakeholder questions:
- When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
For this query, I have used aggregation to calculate the quantity_purchased grouped by receipt status. I have also used join operation to combine 2 tables along with th filter criteria that would limit analysis to only receipts with status 'Accepted' or 'Rejected'. 
```sql
SELECT receipt.rewards_receipt_status,
      SUM(items.quantity_purchased) AS total_items_purchased
FROM fetch_challenge.receipts_data receipt
JOIN fetch_challenge.receipt_items_data items ON receipt.r_id = items.receipt_id
WHERE receipt.rewards_receipt_status IN ('Accepted', 'Rejected')
GROUP BY receipt.rewards_receipt_status;
```

- When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
This query calculates the average spend from receipts with a rewards_receipt_status of either 'Accepted' or 'Rejected' and determines which status has the higher average spend.
```sql
SELECT rewards_receipt_status as greater_status,
      AVG(total_spent) AS average_spend
FROM fetch_challenge.receipts_data
WHERE rewards_receipt_status IN ('Accepted', 'Rejected')
GROUP BY rewards_receipt_status;
```



## Part three: Evaluate data quality issues in the given dataset.
To identify data quality issues, I adopted a systematic approach using Python for data loading, normalization, and evaluation of potential data quality problems. The key steps in my approach included examining missing values, detecting duplicate records, and checking for inconsistencies in data formats. The python script could be found inside Data Quality/data_quality

## Part four: Communicate with Stakeholder.
As the final part of this challenged, I have crafted a professional email to effectively communicate with stakeholders. My email covers the details on some questions about the data,
identification of Data Quality Issues, information needed to resolve Data Quality Issues, support for Data Asset optimzation and anticipated performance and scaling concerns. My email is attached under stakeholder_communication_email/email_communication


