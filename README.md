# fetch-rewards-analysis
Designing a structured data model, writing SQL queries to answer business questions and identifying data quality issues for Fetch Rewards.

## Overview
This is a small project on reviewing unstructure .json file and designging a structured relational data model using dbdiagram.io along with writing SQL queries to answer a few key business questions, identifying data quality issues such as missing values, duplicates, formatting, etc.. and communicating key insights to stakeholders.

## Project Breakdown:
- After reviewing unstructured data, developed a new Relational Data Model to represent how the data could be modeled in a data warehouse.
- SQL queries written to answer following business questions.
   - What are the top 5 brands by receipts scanned for most recent month?
   - How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?
   - When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
- Identifying data quality issues using python and pandas.
- A well-written constructed email to business leader explaining my findings and approach to address the performance and anticipated scaling concerns 

## Technologies Used:

- Python, Pandas
- SQL: PostgreSQL for data analysis
- Diagram Tool: dbdiagram.io
