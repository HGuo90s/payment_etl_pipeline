import sys, uuid
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.context import DynamicFrameCollection
from awsglue.transforms import Join
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, BooleanType
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
from pyspark.sql import Row

# data preprocessing
def data_preprocessing(df): 
    # Perform preprocessing, drop null and convert data types. 
    print("Data Preprocessing...")
    # Drop rows with null Order_Number
    df = df.filter(df["Order_Number"].isNotNull())
    # Convert column names to lowercase
    for col in df.columns:
        df = df.withColumnRenamed(col, col.lower())
    # Convert order_date to proper date type
    # In PySpark, we use to_date function instead of pandas to_datetime
    df = df.withColumn("order_date", 
                      F.to_date(F.col("order_date"), "dd/MM/yyyy"))
    # Convert back to DynamicFrame and return
    return df

# process date dimension
def proc_date_dim(df, glueContext, spark):
    # create and process date dimension
    print("Processing date dimension...")
    # Extract unique dates - need to ensure order_date column exists and is a date type
    if "order_date" not in df.columns:
        raise ValueError("order_date column not found in the input data")
    # Select distinct dates and create a dataframe with just the dates
    dates_df = df.select("order_date").distinct()
    dates_df = dates_df.withColumnRenamed("order_date", "date_full")
    # Add date attributes - similar to pandas operations but using Spark functions
    dates_df = dates_df.withColumn("day_of_week", F.dayofweek("date_full"))
    dates_df = dates_df.withColumn("day_name", F.date_format("date_full", "EEEE"))
    dates_df = dates_df.withColumn("day_of_month", F.dayofmonth("date_full"))
    dates_df = dates_df.withColumn("day_of_year", F.dayofyear("date_full"))
    dates_df = dates_df.withColumn("week_of_year", F.weekofyear("date_full"))
    dates_df = dates_df.withColumn("month_num", F.month("date_full"))
    dates_df = dates_df.withColumn("month_name", F.date_format("date_full", "MMMM"))
    dates_df = dates_df.withColumn("quarter", F.quarter("date_full"))
    dates_df = dates_df.withColumn("year", F.year("date_full"))
    # Check if day_of_week is Saturday (7) or Sunday (1)
    # In Spark SQL, dayofweek returns 1-7 where 1=Sunday, 7=Saturday
    dates_df = dates_df.withColumn(
        "is_weekend", 
        (F.col("day_of_week") == 1) | (F.col("day_of_week") == 7)
    )
    # Convert back to DynamicFrame and return
    return DynamicFrame.fromDF(dates_df, glueContext, "date_dimension")

# process customer dimension 
def proc_cust_dim(df, glueContext):
    print("Processing customer dimension...")
    # Extract unique customer names
    customers_df = df.select("customer_name").distinct()
    # Create surrogate keys using row_number() window function
    window_spec = Window.orderBy("customer_name")
    customers_df = customers_df.withColumn("customer_id", 
                                          F.row_number().over(window_spec))
    # Split customer name into first and last name
    customers_df = customers_df.withColumn("first_name", 
                                          F.split(F.col("customer_name"), " ").getItem(0))
    customers_df = customers_df.withColumn("last_name", 
                                          F.expr("split(customer_name, ' ')[size(split(customer_name, ' '))-1]"))
    # Add metadata columns with current timestamp
    current_timestamp = F.current_timestamp()
    customers_df = customers_df.withColumn("create_date", current_timestamp)
    customers_df = customers_df.withColumn("update_date", current_timestamp)
    # Convert back to DynamicFrame and return
    return DynamicFrame.fromDF(customers_df, glueContext, "customer_dimension")

# process geo dimensions 
def generate_india_states_list(spark):
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
    # Add UUID for each state and territory and country
    for state in india_states:
        state['state_id'] = str(uuid.uuid4())
        state['country'] = 'India'
    # Convert to dataframe
    rows = [Row(**state) for state in india_states] 
    india_state_df = spark.createDataFrame(rows)
    # Reorder columns to make ID first
    columns_order = ['state_id', 'state_code', 'country', 'state_name', 
                     'capital_city', 'status', 'iso_code']
    india_state_df = india_state_df.select(columns_order)
    # Convert to DynamicFrame and return
    return india_state_df

# generate a list of U.S. states
def generate_us_states_list(spark):
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
        state['country'] = 'United States'
    # Convert to dataframe
    rows = [Row(**state) for state in us_states]
    us_state_df = spark.createDataFrame(rows) 
    # Reorder columns to make ID first
    columns_order = ['state_id', 'state_code', 'country', 'state_name', 
                     'capital_city', 'status', 'iso_code']
    us_state_df = us_state_df.select(columns_order)
    return us_state_df

# generate a list of canadian provinces 
def generate_canadian_provinces_list(spark):
    # Create list of province/territory data
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
         "capital_city": "Charlottetown", "status": "Province", 
         "iso_code": "CA-PE"},
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
    # Add UUID for each province/territory and country
    for province in canada_provinces:
        province['state_id'] = str(uuid.uuid4())
        province['country'] = 'Canada'
    # Convert to dataframe
    rows = [Row(**province) for province in canada_provinces]
    canada_provinces_df = spark.createDataFrame(rows)
    # Reorder columns to make ID first
    columns_order = ['state_id', 'state_code', 'country', 'state_name', 
                     'capital_city', 'status', 'iso_code']
    canada_provinces_df = canada_provinces_df.select(columns_order)
    # Convert to DynamicFrame and return
    return canada_provinces_df

# based on the list generated above
# obtain a comprehensive list of states in U.S., Canada, and India.
def proc_geo_dim(spark, glueContext): 
    print('Processing geography dimension...')
    # Generate states/provinces for India, U.S., and Canada
    geographys_india = generate_india_states_list(spark)
    geographys_us = generate_us_states_list(spark)
    geographys_canada = generate_canadian_provinces_list(spark)
    # Concatenate DataFrames
    geographys = geographys_india.union(geographys_us).union(geographys_canada)
    # add metadata
    geographys = geographys.withColumn("created_at", F.current_timestamp())
    # Cache the result to improve performance
    geographys_df = geographys.cache()
    return DynamicFrame.fromDF(geographys_df, glueContext, "geography_dimension")

# process product dimension
def proc_prod_dim(df, glueContext):
    print('Processing product dimension...')
    # Extract unique product information
    products = df.select('product', 'category', 'brand').distinct()
    # Add surrogate key using row_number window function
    window_spec = Window.orderBy('product', 'category', 'brand')
    products = products.withColumn('product_id', F.row_number().over(window_spec))
    # Get unique product-cost combinations
    pc = df.select('product', 'category', 'brand', 'cost').distinct()
    # Ensure there is only one cost associated with each product
    # Count unique costs per product
    pc_counts = pc.groupBy('product', 'category', 'brand').agg(
        F.countDistinct('cost').alias('costcnt')
    )
    # Filter to keep only products with a single cost value
    pc_valid = pc_counts.filter(F.col('costcnt') == 1)
    # Join back to get the cost value
    pc = pc.join(pc_valid, on=['product', 'category', 'brand'], how='inner')
    pc = pc.drop('costcnt')
    # Merge cost information with product table
    products = products.join(
        pc.select('product', 'category', 'brand', 'cost'),
        on=['product', 'category', 'brand'],
        how='left'
    )
    # Rename columns
    products = products.withColumnRenamed('product', 'product_name')
    products = products.withColumnRenamed('cost', 'standard_cost')
    # Fill null values with 0 for standard_cost
    products = products.na.fill({'standard_cost': 0})
    # Add metadata columns
    current_timestamp = F.current_timestamp()
    products = products.withColumn('create_date', current_timestamp)
    products = products.withColumn('update_date', current_timestamp)
    return DynamicFrame.fromDF(products, glueContext, "product_dimension")


def proc_ostatus_dim(df, glueContext): 
    print('Processing order status dimension...')
    # Extract unique statuses
    status = df.select('status').distinct()
    # Add surrogate key 
    window_spec = Window.orderBy('status')
    status = status.withColumn('status_id', F.row_number().over(window_spec))
    # Rename columns
    status = status.withColumnRenamed('status', 'status_name')
    
    # Instead of using a UDF, use when/otherwise for mapping
    status = status.withColumn(
        'status_description',
        F.when(F.col('status_name') == 'Delivered', 'Order has been delivered')
         .when(F.col('status_name') == 'Order', 'Order has been placed')
         .when(F.col('status_name') == 'Processing', 'Order is being processed')
         .when(F.col('status_name') == 'Shipped', 'Order has been shipped')
         .otherwise('Unknown status')
    )
    
    # Add metadata columns
    current_timestamp = F.current_timestamp()
    status = status.withColumn('create_date', current_timestamp)
    status = status.withColumn('update_date', current_timestamp)
    return DynamicFrame.fromDF(status, glueContext, "status_dimension")

# process employee/supervisor dimension
def proc_emp_dim(df, glueContext): 
    print('Processing employee/supervisor dimension...')
    # Extract unique supervisor names
    employee = df.select('assigned supervisor').distinct()
    # Add surrogate key
    window_spec = Window.orderBy('assigned supervisor')
    employee = employee.withColumn('employee_id', F.row_number().over(window_spec))
    # Rename columns
    employee = employee.withColumnRenamed('assigned supervisor', 'employee_name')
    # Split names to get first and last names
    employee = employee.withColumn('employee_first_name', 
                                 F.split(F.col('employee_name'), ' ').getItem(0))
    employee = employee.withColumn('employee_last_name',
                                 F.expr("split(employee_name, ' ')[size(split(employee_name, ' '))-1]"))
    # Add metadata columns
    current_timestamp = F.current_timestamp()
    employee = employee.withColumn('create_date', current_timestamp)
    employee = employee.withColumn('update_date', current_timestamp)
    # Convert back to DynamicFrame and return
    return DynamicFrame.fromDF(employee, glueContext, "employee_dimension")


def fact_table(df, dates, customers, geographys, products, status, employee, glueContext, spark): 
    print("Creating fact table...")
    # First check what we're working with
    # Use the DataFrame directly without trying to convert it
    print("Using orders DataFrame directly")
    orders_df = df  # Already a DataFrame, no conversion needed
    # Only convert the dimension DynamicFrames to DataFrames
    print("Converting dimension DynamicFrames to DataFrames")
    customers_df = customers.toDF()
    geo_df = geographys.toDF()
    products_df = products.toDF()
    status_df = status.toDF()
    employee_df = employee.toDF()

    # Pre-filter and select only needed columns from geo dimension
    print("Filtering geography to India")
    geo_india_df = geo_df.filter(F.col("country") == "India").select("state_code", "state_id")
    
    # Prepare all dimension DataFrames with only the columns needed for joining
    print("Preparing dimension tables for joins")
    customers_join_df = customers_df.select("customer_name", "customer_id")
    products_join_df = products_df.select(F.col("product_name").alias("product"), "product_id")
    status_join_df = status_df.select(F.col("status_name").alias("status"), "status_id")
    employee_join_df = employee_df.select(F.col("employee_name").alias("assigned supervisor"), "employee_id")
    
    # Perform joins - use broadcast for smaller dimension tables
    print("Joining fact table with dimensions")
    orders_df = orders_df.join(F.broadcast(customers_join_df), on="customer_name", how="left")
    orders_df = orders_df.join(F.broadcast(geo_india_df), on="state_code", how="left")
    orders_df = orders_df.join(F.broadcast(products_join_df), on="product", how="left")
    orders_df = orders_df.join(F.broadcast(status_join_df), on="status", how="left")
    orders_df = orders_df.join(F.broadcast(employee_join_df), on="assigned supervisor", how="left")
    
    # Rename columns
    orders_df = orders_df.withColumnRenamed("cost", "unit_cost") \
                         .withColumnRenamed("sales", "unit_sales")
    
    # Calculate derived columns in one step when possible
    print("Calculating derived columns")
    orders_df = orders_df.withColumn("profit", F.col("total_sales") - F.col("total_cost"))
    orders_df = orders_df.withColumn(
        "profit_margin", 
        F.when(F.col("total_sales") > 0, 
              (F.col("total_sales") - F.col("total_cost")) / F.col("total_sales")
        ).otherwise(F.lit(0))
    )
    
    # Select required columns
    required_columns = [
        'order_number', 'order_date', 'customer_id', 'state_id', 
        'product_id', 'status_id', 'employee_id', 'unit_cost', 
        'unit_sales', 'quantity', 'total_cost', 'total_sales', 
        'profit', 'profit_margin'
    ]
    orders_df = orders_df.select(required_columns)
    
    # Handle missing values in one operation
    print("Handling missing values")
    null_defaults = {
        "customer_id": -1, "state_id": -1, "product_id": -1,
        "status_id": -1, "employee_id": -1, "unit_cost": 0,
        "unit_sales": 0, "total_cost": 0, "total_sales": 0,
        "profit": 0, "profit_margin": 0
    }
    orders_df = orders_df.na.fill(null_defaults)
    
    # Convert to DynamicFrame after all DataFrame operations are complete
    print("Converting to DynamicFrame")
    return DynamicFrame.fromDF(orders_df, glueContext, "orders_fact_table")

def save_dfs_to_s3(glueContext, dataframes, file_names, 
bucket_name, folder_path, format="parquet"):
    print('Saving Data to S3...')
    # Verify inputs are valid
    if len(dataframes) != len(file_names):
        raise ValueError(f"""Number of dataframes ({len(dataframes)}) \
        must match number of file names ({len(file_names)})""")
    # Loop through dataframes and save each one
    for i, (df, file_name) in enumerate(zip(dataframes, file_names)):
        print(f"Saving {file_name} to S3 ({i+1}/{len(dataframes)})...")
        # Create S3 path
        s3_path = f"s3://{bucket_name}/{folder_path}{file_name}"
        
        # Convert to regular DataFrame and write using Spark's write method
        spark_df = df.toDF()
        spark_df.write.mode("overwrite").parquet(s3_path)
        
        print(f"Successfully saved {file_name} to {s3_path}")
    print(f"""All {len(dataframes)} dataframes saved successfully \
    to s3://{bucket_name}/{folder_path}""")

def main(): 
    print("Starting ETL job...")
    # Initialize Glue job
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # customize the environment
    source_database = "raw_ecommerce_db"
    source_table = "online_ecommerce_csv"
    target_bucket = "aws-bucket-ecommerce"
    target_folder = "processed/"
    
    print(f"Reading data from {source_database}.{source_table}...")
    # Read data from catalog
    try:
        raw_order = glueContext.create_dynamic_frame.from_catalog(
            database=source_database,
            table_name=source_table,
            transformation_ctx="source_data"
        )
        
        # convert to dataframe
        raw_order_df = raw_order.toDF()
        print(f"Successfully read data: {raw_order_df.count()} rows")
        
        # preprocessing 
        df = data_preprocessing(raw_order_df)
        print(f"Preprocessed data: {df.count()} rows")
        print(f"Columns: {df.columns}")
        
        # transform and create dimensions
        dim_date = proc_date_dim(df, glueContext, spark)
        dim_cust = proc_cust_dim(df, glueContext)
        dim_geo = proc_geo_dim(spark, glueContext)
        dim_prod = proc_prod_dim(df, glueContext)
        dim_ostatus = proc_ostatus_dim(df, glueContext)
        dim_emp = proc_emp_dim(df, glueContext)
        
        # transform fact table, orders
        fact_orders = fact_table(df, dim_date, dim_cust, dim_geo, 
             dim_prod, dim_ostatus, dim_emp, glueContext, spark)
        
        # Prepare for saving
        files = [dim_date, dim_cust, dim_geo, dim_prod, dim_ostatus, 
                 dim_emp, fact_orders]
        file_names = ['dim_date', 'dim_cust', 'dim_geo', 'dim_prod', 
                      'dim_ostatus', 'dim_emp', 'fact_orders']
        # Save all dataframes to S3
        save_dfs_to_s3(
            glueContext=glueContext,  # This is now correctly passed
            dataframes=files,         # Changed from dfs1 to dataframes
            file_names=file_names,
            bucket_name=target_bucket,
            folder_path=target_folder,
            format="parquet"
        )
        
    except Exception as e:
        print(f"Error in ETL process: {str(e)}")
        raise
        
    job.commit()
    print("ETL job completed successfully")

# Add this to the end of your script to call the main function
if __name__ == "__main__":
    main()