import numpy as np
import pandas as pd
import sys, io, uuid
from datetime import datetime
import requests
from io import StringIO

# This is a sample ETL implementation to process the datafile
# from the database using Python files. 
# This file will read from either DB connection or GitHub

# define the initialization function


# load raw data from github
def load_data_from_github(github_url):
    """Load data from GitHub"""
    print(f"Loading data from GitHub: {github_url}")
    # For raw GitHub content, convert from regular GitHub URL to raw URL
    if 'github.com' in github_url and '/blob/' in github_url:
        github_url = github_url.replace('github.com', 
                     'raw.githubusercontent.com').replace('/blob/', '/')
    # Download the CSV file from GitHub
    response = requests.get(github_url)

    # Check if the request was successful
    if response.status_code == 200:
        # Convert the content to a StringIO object
        data_string = StringIO(response.text)
        
        # Read the CSV into a pandas DataFrame
        df = pd.read_csv(data_string)
        print(f"""Successfully loaded data: 
        {df.shape[0]} rows and {df.shape[1]} columns""")
        return df
    else: 
        raise Exception(f"""Failed to load data: HTTP status code 
        {response.status_code}""")


# data preprocessing
def data_preprocessing(df): 
    "Perform preprocessing, drop null and convert data types"
    print("Data Preprocessing...")
    
    # creating a copy
    df1 = df.dropna(subset=['Order_Number'])
    df2 = df1.copy()
    df2.columns = df2.columns.str.lower()
    df2['order_date'] = pd.to_datetime(df2['order_date'], format='%d/%m/%Y')
    return df2

# process data dimension
def proc_date_dim(df):
    "Process the date dimension "
    print("Processing date dimension...")
    # create date dimensions
    dates = pd.DataFrame({'date_full': 
                      pd.to_datetime(df['order_date'].unique())})
    # Add date attributes
    dates['day_of_week'] = dates['date_full'].dt.dayofweek + 1
    dates['day_name'] = dates['date_full'].dt.day_name()
    dates['day_of_month'] = dates['date_full'].dt.day
    dates['day_of_year'] = dates['date_full'].dt.dayofyear
    dates['week_of_year'] = dates['date_full'].dt.isocalendar().week
    dates['month_num'] = dates['date_full'].dt.month
    dates['month_name'] = dates['date_full'].dt.month_name()
    dates['quarter'] = dates['date_full'].dt.quarter
    dates['year'] = dates['date_full'].dt.year
    dates['is_weekend'] = dates['day_of_week'].isin([6, 7])
    
    return dates

# process customer dimension
def proc_cust_dim(df):
    # process the customer dimension"
    print('Processing customer dimension...')
    # extract unique customer names
    customers = pd.DataFrame({'customer_name':
                          df['customer_name'].unique()})
    # create surrogate keys
    customers['customer_id'] = range(1, len(customers)+1)
    # add customer attributes
    customers['first_name'] = customers['customer_name'].str.split().str[0]
    customers['last_name'] = customers['customer_name'].str.split().str[-1]
    # add metadata
    customers['create_date'] = pd.to_datetime('now')
    customers['update_date'] = pd.to_datetime('now')
    
    return customers

# process geography dimension
# generate list of Indian, U.S., and Canadian states
# generate Indian states
def generate_india_states_list():
    # Create list of Indian states and union territories with their details
    india_states = [
        {"state_code": "AN", "state_name": "Andaman and Nicobar Islands", 
         "capital_city": "Port Blair", "status": "Union Territory", 
         "iso_code": "IN-AN"},
        {"state_code": "AP", "state_name": "Andhra Pradesh", 
         "capital_city": "Amaravati", "status": "State", 
         "iso_code": "IN-AP"},
        {"state_code": "AR", "state_name": "Arunachal Pradesh", 
         "capital_city": "Itanagar", "status": "State", "iso_code": "IN-AR"},
        {"state_code": "AS", "state_name": "Assam", "capital_city": "Dispur", 
         "status": "State", "iso_code": "IN-AS"},
        {"state_code": "BR", "state_name": "Bihar", "capital_city": "Patna", 
         "status": "State", "iso_code": "IN-BR"},
        {"state_code": "CH", "state_name": "Chandigarh", 
         "capital_city": "Chandigarh", "status": "Union Territory", 
         "iso_code": "IN-CH"},
        {"state_code": "CG", "state_name": "Chhattisgarh", 
         "capital_city": "Raipur", "status": "State", "iso_code": "IN-CG"},
        {"state_code": "DD", 
         "state_name": "Daman and Diu", 
         "capital_city": "Daman", "status": "Union Territory", 
         "iso_code": "IN-DD"},
        {"state_code": "DH", 
         "state_name": "Dadra and Nagar Haveli and Daman and Diu", 
         "capital_city": "Daman", "status": "Union Territory", 
         "iso_code": "IN-DH"},
        {"state_code": "DL", "state_name": "Delhi", 
         "capital_city": "New Delhi", "status": "Union Territory", 
         "iso_code": "IN-DL"},
        {"state_code": "GA", "state_name": "Goa", "capital_city": "Panaji", 
         "status": "State", "iso_code": "IN-GA"},
        {"state_code": "GJ", "state_name": "Gujarat", 
         "capital_city": "Gandhinagar", "status": "State", "iso_code": "IN-GJ"},
        {"state_code": "HR", "state_name": "Haryana", 
         "capital_city": "Chandigarh", "status": "State", "iso_code": "IN-HR"},
        {"state_code": "HP", "state_name": "Himachal Pradesh", 
         "capital_city": "Shimla", "status": "State", "iso_code": "IN-HP"},
        {"state_code": "JK", "state_name": "Jammu and Kashmir", 
         "capital_city": "Srinagar/Jammu", "status": "Union Territory", 
         "iso_code": "IN-JK"},
        {"state_code": "JH", "state_name": "Jharkhand", 
         "capital_city": "Ranchi", "status": "State", "iso_code": "IN-JH"},
        {"state_code": "KA", "state_name": "Karnataka", 
         "capital_city": "Bengaluru", "status": "State", "iso_code": "IN-KA"},
        {"state_code": "KL", "state_name": "Kerala", 
         "capital_city": "Thiruvananthapuram", "status": "State", 
         "iso_code": "IN-KL"},
        {"state_code": "LA", "state_name": "Ladakh", 
         "capital_city": "Leh", "status": "Union Territory", 
         "iso_code": "IN-LA"},
        {"state_code": "LD", "state_name": "Lakshadweep", 
         "capital_city": "Kavaratti", "status": "Union Territory", 
         "iso_code": "IN-LD"},
        {"state_code": "MP", "state_name": "Madhya Pradesh", 
         "capital_city": "Bhopal", "status": "State", "iso_code": "IN-MP"},
        {"state_code": "MH", "state_name": "Maharashtra", 
         "capital_city": "Mumbai", "status": "State", "iso_code": "IN-MH"},
        {"state_code": "MN", "state_name": "Manipur", 
         "capital_city": "Imphal", "status": "State", "iso_code": "IN-MN"},
        {"state_code": "ML", "state_name": "Meghalaya", 
         "capital_city": "Shillong", "status": "State", "iso_code": "IN-ML"},
        {"state_code": "MZ", "state_name": "Mizoram", 
         "capital_city": "Aizawl", "status": "State", "iso_code": "IN-MZ"},
        {"state_code": "NL", "state_name": "Nagaland", 
         "capital_city": "Kohima", "status": "State", "iso_code": "IN-NL"},
        {"state_code": "OD", "state_name": "Odisha", 
         "capital_city": "Bhubaneswar", "status": "State", 
         "iso_code": "IN-OD"},
        {"state_code": "OR", "state_name": "Orissa", 
         "capital_city": "Bhubaneswar", "status": "State", 
         "iso_code": "IN-OR"},
        {"state_code": "PY", "state_name": "Puducherry", 
         "capital_city": "Puducherry", "status": "Union Territory", 
         "iso_code": "IN-PY"},
        {"state_code": "PB", "state_name": "Punjab", 
         "capital_city": "Chandigarh", "status": "State", 
         "iso_code": "IN-PB"},
        {"state_code": "RJ", "state_name": "Rajasthan", 
         "capital_city": "Jaipur", "status": "State", "iso_code": "IN-RJ"},
        {"state_code": "SK", "state_name": "Sikkim", 
         "capital_city": "Gangtok", "status": "State", "iso_code": "IN-SK"},
        {"state_code": "TN", "state_name": "Tamil Nadu", 
         "capital_city": "Chennai", "status": "State", "iso_code": "IN-TN"},
        {"state_code": "TG", "state_name": "Telangana", 
         "capital_city": "Hyderabad", "status": "State", "iso_code": "IN-TS"},
        {"state_code": "TR", "state_name": "Tripura", 
         "capital_city": "Agartala", "status": "State", "iso_code": "IN-TR"},
        {"state_code": "UP", "state_name": "Uttar Pradesh", 
         "capital_city": "Lucknow", "status": "State", "iso_code": "IN-UP"},
        {"state_code": "UK", "state_name": "Uttarakhand", 
         "capital_city": "Dehradun", "status": "State", "iso_code": "IN-UK"},
        {"state_code": "WB", "state_name": "West Bengal", 
         "capital_city": "Kolkata", "status": "State", "iso_code": "IN-WB"}
    ]
    # Add UUID for each state/territory
    for state in india_states:
        state['state_id'] = str(uuid.uuid4())
    # Convert to DataFrame for easy manipulation/export
    df = pd.DataFrame(india_states)
    df['country'] = 'India'
    # Reorder columns to make ID first
    df = df[['state_id', 'state_code', 'country', 'state_name', 
             'capital_city', 'status', 'iso_code']]
    
    return df

def generate_us_states_list():
    # Create list of U.S. states and territories with their details
    us_states = [
        {"state_code": "AL", "state_name": "Alabama", 
         "capital_city": "Montgomery", "status": "State", "iso_code": "US-AL"},
        {"state_code": "AK", "state_name": "Alaska", 
         "capital_city": "Juneau", "status": "State", "iso_code": "US-AK"},
        {"state_code": "AZ", "state_name": "Arizona", 
         "capital_city": "Phoenix", "status": "State", "iso_code": "US-AZ"},
        {"state_code": "AR", "state_name": "Arkansas", 
         "capital_city": "Little Rock", "status": "State", 
         "iso_code": "US-AR"},
        {"state_code": "CA", "state_name": "California", 
         "capital_city": "Sacramento", "status": "State", 
         "iso_code": "US-CA"},
        {"state_code": "CO", "state_name": "Colorado", 
         "capital_city": "Denver", "status": "State", "iso_code": "US-CO"},
        {"state_code": "CT", "state_name": "Connecticut", 
         "capital_city": "Hartford", "status": "State", "iso_code": "US-CT"},
        {"state_code": "DE", "state_name": "Delaware", 
         "capital_city": "Dover", "status": "State", "iso_code": "US-DE"},
        {"state_code": "FL", "state_name": "Florida", 
         "capital_city": "Tallahassee", "status": "State", 
         "iso_code": "US-FL"},
        {"state_code": "GA", "state_name": "Georgia", 
         "capital_city": "Atlanta", "status": "State", "iso_code": "US-GA"},
        {"state_code": "HI", "state_name": "Hawaii", 
         "capital_city": "Honolulu", "status": "State", "iso_code": "US-HI"},
        {"state_code": "ID", "state_name": "Idaho", 
         "capital_city": "Boise", "status": "State", "iso_code": "US-ID"},
        {"state_code": "IL", "state_name": "Illinois", 
         "capital_city": "Springfield", "status": "State", 
         "iso_code": "US-IL"},
        {"state_code": "IN", "state_name": "Indiana", 
         "capital_city": "Indianapolis", "status": "State", 
         "iso_code": "US-IN"},
        {"state_code": "IA", "state_name": "Iowa", 
         "capital_city": "Des Moines", "status": "State", "iso_code": "US-IA"},
        {"state_code": "KS", "state_name": "Kansas", 
         "capital_city": "Topeka", "status": "State", "iso_code": "US-KS"},
        {"state_code": "KY", "state_name": "Kentucky", 
         "capital_city": "Frankfort", "status": "State", "iso_code": "US-KY"},
        {"state_code": "LA", "state_name": "Louisiana", 
         "capital_city": "Baton Rouge", "status": "State", 
         "iso_code": "US-LA"},
        {"state_code": "ME", "state_name": "Maine", 
         "capital_city": "Augusta", "status": "State", "iso_code": "US-ME"},
        {"state_code": "MD", "state_name": "Maryland", 
         "capital_city": "Annapolis", "status": "State", "iso_code": "US-MD"},
        {"state_code": "MA", "state_name": "Massachusetts", 
         "capital_city": "Boston", "status": "State", "iso_code": "US-MA"},
        {"state_code": "MI", "state_name": "Michigan", 
         "capital_city": "Lansing", "status": "State", "iso_code": "US-MI"},
        {"state_code": "MN", "state_name": "Minnesota", 
         "capital_city": "St. Paul", "status": "State", "iso_code": "US-MN"},
        {"state_code": "MS", "state_name": "Mississippi", 
         "capital_city": "Jackson", "status": "State", "iso_code": "US-MS"},
        {"state_code": "MO", "state_name": "Missouri", 
         "capital_city": "Jefferson City", "status": "State", 
         "iso_code": "US-MO"},
        {"state_code": "MT", "state_name": "Montana", 
         "capital_city": "Helena", "status": "State", "iso_code": "US-MT"},
        {"state_code": "NE", "state_name": "Nebraska", 
         "capital_city": "Lincoln", "status": "State", "iso_code": "US-NE"},
        {"state_code": "NV", "state_name": "Nevada", 
         "capital_city": "Carson City", "status": "State", 
         "iso_code": "US-NV"},
        {"state_code": "NH", "state_name": "New Hampshire", 
         "capital_city": "Concord", "status": "State", "iso_code": "US-NH"},
        {"state_code": "NJ", "state_name": "New Jersey", 
         "capital_city": "Trenton", "status": "State", "iso_code": "US-NJ"},
        {"state_code": "NM", "state_name": "New Mexico", 
         "capital_city": "Santa Fe", "status": "State", "iso_code": "US-NM"},
        {"state_code": "NY", "state_name": "New York", 
         "capital_city": "Albany", "status": "State", "iso_code": "US-NY"},
        {"state_code": "NC", "state_name": "North Carolina", 
         "capital_city": "Raleigh", "status": "State", "iso_code": "US-NC"},
        {"state_code": "ND", "state_name": "North Dakota", 
         "capital_city": "Bismarck", "status": "State", "iso_code": "US-ND"},
        {"state_code": "OH", "state_name": "Ohio", 
         "capital_city": "Columbus", "status": "State", "iso_code": "US-OH"},
        {"state_code": "OK", "state_name": "Oklahoma", 
         "capital_city": "Oklahoma City", "status": "State", 
         "iso_code": "US-OK"},
        {"state_code": "OR", "state_name": "Oregon", 
         "capital_city": "Salem", "status": "State", "iso_code": "US-OR"},
        {"state_code": "PA", "state_name": "Pennsylvania", 
         "capital_city": "Harrisburg", "status": "State", "iso_code": "US-PA"},
        {"state_code": "RI", "state_name": "Rhode Island", 
         "capital_city": "Providence", "status": "State", "iso_code": "US-RI"},
        {"state_code": "SC", "state_name": "South Carolina", 
         "capital_city": "Columbia", "status": "State", "iso_code": "US-SC"},
        {"state_code": "SD", "state_name": "South Dakota", 
         "capital_city": "Pierre", "status": "State", "iso_code": "US-SD"},
        {"state_code": "TN", "state_name": "Tennessee", 
         "capital_city": "Nashville", "status": "State", "iso_code": "US-TN"},
        {"state_code": "TX", "state_name": "Texas", 
         "capital_city": "Austin", "status": "State", "iso_code": "US-TX"},
        {"state_code": "UT", "state_name": "Utah", 
         "capital_city": "Salt Lake City", "status": "State", 
         "iso_code": "US-UT"},
        {"state_code": "VT", "state_name": "Vermont", 
         "capital_city": "Montpelier", "status": "State", "iso_code": "US-VT"},
        {"state_code": "VA", "state_name": "Virginia", 
         "capital_city": "Richmond", "status": "State", "iso_code": "US-VA"},
        {"state_code": "WA", "state_name": "Washington", 
         "capital_city": "Olympia", "status": "State", "iso_code": "US-WA"},
        {"state_code": "WV", "state_name": "West Virginia", 
         "capital_city": "Charleston", "status": "State", "iso_code": "US-WV"},
        {"state_code": "WI", "state_name": "Wisconsin", 
         "capital_city": "Madison", "status": "State", "iso_code": "US-WI"},
        {"state_code": "WY", "state_name": "Wyoming", 
         "capital_city": "Cheyenne", "status": "State", "iso_code": "US-WY"},
        {"state_code": "DC", "state_name": "District of Columbia", 
         "capital_city": "Washington", "status": "Federal District", 
         "iso_code": "US-DC"},
        {"state_code": "AS", "state_name": "American Samoa", 
         "capital_city": "Pago Pago", "status": "Territory", 
         "iso_code": "US-AS"},
        {"state_code": "GU", "state_name": "Guam", 
         "capital_city": "Hagåtña", "status": "Territory", 
         "iso_code": "US-GU"},
        {"state_code": "MP", "state_name": "Northern Mariana Islands", 
         "capital_city": "Saipan", "status": "Territory", "iso_code": "US-MP"},
        {"state_code": "PR", "state_name": "Puerto Rico", 
         "capital_city": "San Juan", "status": "Territory", 
         "iso_code": "US-PR"},
        {"state_code": "VI", "state_name": "U.S. Virgin Islands", 
         "capital_city": "Charlotte Amalie", "status": "Territory", 
         "iso_code": "US-VI"}
    ]
        
    # Add UUID for each state/territory
    for state in us_states:
        state['state_id'] = str(uuid.uuid4())
    
    # Convert to DataFrame for easy manipulation/export
    df = pd.DataFrame(us_states)
    df['country'] = 'United States'
    
    # Reorder columns to make ID first
    df = df[['state_id', 'state_code', 'country', 'state_name', 
             'capital_city', 'status', 'iso_code']]
    
    return df

def generate_canadian_provinces_list():
    # Canadian Provinces and Territories
    canada_provinces = [
        {"state_code": "AB", "state_name": "Alberta", 
         "capital_city": "Edmonton", "status": "Province",
         "iso_code": "CA-AB"},
        {"state_code": "BC", "state_name": "British Columbia", 
         "capital_city": "Victoria", "status": "Province",
         "iso_code": "CA-BC"},
        {"state_code": "MB", "state_name": "Manitoba", 
         "capital_city": "Winnipeg", "status": "Province", 
         "iso_code": "CA-MB"},
        {"state_code": "NB", "state_name": "New Brunswick", 
         "capital_city": "Fredericton", "status": "Province", 
         "iso_code": "CA-NB"},
        {"state_code": "NL", "state_name": "Newfoundland and Labrador", 
         "capital_city": "St. John's", "status": "Province", 
         "iso_code": "CA-NL"},
        {"state_code": "NS", "state_name": "Nova Scotia", 
         "capital_city": "Halifax", "status": "Province", 
         "iso_code": "CA-NS"},
        {"state_code": "ON", "state_name": "Ontario", 
         "capital_city": "Toronto", "status": "Province", 
         "iso_code": "CA-ON"},
        {"state_code": "PE", "state_name": "Prince Edward Island", 
         "status": "Province", "iso_code": "CA-PE"},
        {"state_code": "QC", "state_name": "Quebec", 
         "capital_city": "Quebec City", "status": "Province",
         "iso_code": "CA-QC"},
        {"state_code": "SK", "state_name": "Saskatchewan", 
         "capital_city": "Regina", "status": "Province", 
         "iso_code": "CA-SK"},
        {"state_code": "NT", "state_name": "Northwest Territories", 
         "capital_city": "Yellowknife", "status": "Territory",
         "iso_code": "CA-NT"},
        {"state_code": "NU", "state_name": "Nunavut", 
         "capital_city": "Iqaluit", "status": "Territory",
         "iso_code": "CA-NU"},
        {"state_code": "YT", "state_name": "Yukon", 
         "capital_city": "Whitehorse", "status": "Territory",
         "iso_code": "CA-YT"}
    ]
    
    # Add UUID for each state/territory
    for state in canada_provinces:
        state['state_id'] = str(uuid.uuid4())
    
    # Convert to DataFrame for easy manipulation/export
    df = pd.DataFrame(canada_provinces)
    df['country'] = 'Canada'

        # Reorder columns to make ID first
    df = df[['state_id', 'state_code', 'country', 'state_name', 
             'capital_city', 'status', 'iso_code']]
    
    return df

# based on the list generated above
# obtain a comprehensive list of states in U.S., Canada, and India.
def proc_geo_dim(): 
    print('Processing geography dimension...')
    # generate states for india, u.s., and canada
    geographys_india = generate_india_states_list()
    geographys_us = generate_us_states_list()
    geographys_canada = generate_canadian_provinces_list()
    # concatenate them into one.
    geographys = pd.concat([geographys_india, geographys_us, 
                            geographys_canada])
    
    return geographys

# process product dimension
def proc_prod_dim(df):
    # process product table
    print('Processing product dimension...')
    products = df[['product', 'category', 'brand']].drop_duplicates()
    # add a surrogate key
    products['product_id'] = range(1, len(products)+1)
    # add the standard cost to the product table
    pc = df[['product', 'category', 'brand', 'cost']].drop_duplicates()
    # ensure there is only one cost associated with each product
    pc1 = pc.groupby(['product', 'category', 'brand']).agg(
        costcnt=('cost', 'nunique') )
    pc1 = pc1[pc1['costcnt'] == 1]
    pc = pd.merge(pc, pc1, on=['product', 'category', 'brand'], how='inner')
    pc = pc.drop(columns=['costcnt'])
    # merge with product table
    products = pd.merge(products, pc, 
                        on=['product', 'category', 'brand'], how='left')
    products.columns = ['product_name', 'category', 'brand', 'product_id', 
                        'standard_cost']
    products.fillna(0, inplace=True)
    # add metadata
    products['create_date'] = pd.to_datetime('now')
    products['update_date'] = pd.to_datetime('now')
    
    return products

# process order status dimension
def proc_ostatus_dim(df):
    print('Processing order status dimension...')
    status = df[['status']].drop_duplicates()
    status['status_id'] = range(1, len(status)+1)
    status.columns = ['status_name', 'status_id']
    # add description to order status
    status_descriptions = {
        'Delivered': 'Order has been delivered', 
        'Order': 'Order has been placed', 
        'Processing': 'Order is being processed', 
        'Shipped': 'Order has been shipped'   
    }
    status['status_description'] = \
    status['status_name'].map(status_descriptions)
    # add metadata
    status['create_date'] = pd.to_datetime('now')
    status['update_date'] = pd.to_datetime('now')
    
    return status

# process employee/supervisor dimension
def proc_emp_dim(df): 
    print('Processing employee/supervisor dimension...')
    # add supervisor dimension
    employee = df[['assigned supervisor']].drop_duplicates()
    employee['employee_id'] = range(1, len(employee)+1)
    employee.columns = ['employee_name', 'employee_id']
    # get the first and last names
    employee['employee_first_name'] = \
    employee['employee_name'].str.split().str[0]
    employee['employee_last_name'] = \
    employee['employee_name'].str.split().str[-1]
    # add metadata
    employee['create_date'] = pd.to_datetime('now')
    employee['update_date'] = pd.to_datetime('now')
    
    return employee

# create the fact table of orders
def fact_table(df, dates, customers, geographys, 
               products, status, employee):
    print("Creating fact table...")
    orders = df.copy()
    # create surrogate key mappings from dimension tables
    # for geography, only ensure locations
    customer_key_map = customers.set_index('customer_name')['customer_id'].to_dict()
    geo_subset = geographys[geographys.country=='India']
    geographys_key_map = geo_subset.set_index('state_code')['state_id'].to_dict()
    product_key_map = products.set_index('product_name')['product_id'].to_dict()
    status_key_map = status.set_index('status_name')['status_id'].to_dict()
    employee_key_map = employee.set_index('employee_name')['employee_id'].to_dict()
    # map the keys
    orders['customer_id'] = orders['customer_name'].map(customer_key_map)
    orders['state_id'] = orders['state_code'].map(geographys_key_map)
    orders['product_id'] = orders['product'].map(product_key_map)
    orders['status_id'] = orders['status'].map(status_key_map)
    orders['employee_id'] = orders['assigned supervisor'].map(employee_key_map)
    # rename some columns
    orders = orders.rename(columns={
        'cost': 'unit_cost', 'sales': 'unit_sales' } )
    # calculate derived columns
    orders['profit'] = orders['total_sales'] - orders['total_cost']
    orders['profit_margin'] = orders['profit'] / orders['total_sales']
    # select necessary columns
    required_columns = [
        'order_number', 'order_date', 'customer_id', 'state_id', 
        'product_id', 'status_id', 'employee_id', 'unit_cost', 
        'unit_sales', 'quantity', 'total_cost', 'total_sales', 
        'profit', 'profit_margin'
    ]
    
    return orders[required_columns]

# main ETL function
def run_etl_github(github_url): 
    print("Starting ETL process...")
    # load data from github link
    df = load_data_from_github(github_url)
    # preprocessing 
    df = data_preprocessing(df)
    print(df.columns)
    # transform and create dimensions
    dim_date = proc_date_dim(df)
    dim_cust = proc_cust_dim(df)
    dim_geo = proc_geo_dim()
    dim_prod = proc_prod_dim(df)
    dim_ostatus = proc_ostatus_dim(df)
    dim_emp = proc_emp_dim(df)
    # transform fact table, orders
    fact_orders = fact_table(df, dim_date, dim_cust, dim_geo, 
               dim_prod, dim_ostatus, dim_emp)
    # name the files to be saved
    files = [dim_date, dim_cust, dim_geo, dim_prod, dim_ostatus, 
             dim_emp, fact_orders]
    file_names = ['dim_date', 'dim_cust', 'dim_geo', 'dim_prod', 
                  'dim_ostatus', 'dim_emp', 'fact_orders']
    # save the files locally to parquet files
    comp = 'snappy'       
    try: 
        for i in range(len(files)):
            parquet_file_path = f"{file_names[i]}.parquet"
            csv_file_path = f"{file_names[i]}.csv"
            files[i].to_parquet(parquet_file_path, compression = comp)
            files[i].to_csv(csv_file_path, index = False)
        print("ETL files saved successfully!")
    except Exception as e:
        print(f"Error saving the files: {e}")

# based on github location, save the ETL files locally


# usage
if __name__ == "__main__":
    # Replace with your actual file path and connection string
    data_source = input(
        """Please enter the data source ("github or "database"): """)
    if data_source=='github':
        github_url = input(
            """Please enter the GitHub link: """)
        run_etl_github(github_url)
    elif data_source=='database': 
        db_connection = input(
            """DB connection not supported. Please use github link.""")
    else: 
        raise Exception('Failed to initalize.')