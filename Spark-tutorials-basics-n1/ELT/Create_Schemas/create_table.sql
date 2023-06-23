-- Databricks notebook source
create table if not exists demo.Dim_Country_currency_conversion_xref
(
  Base_Currency_Code string,
  Target_Currency_Code string,
  Conversion_Rate_Today double
)
using delta
location '/mnt/jnj/warehouse/demo/Dim_Country_currency_conversion_xref'

-- COMMAND ----------

create table if not exists demo.Dim_Product_Price_Reference
(
  Ref_Product_Id string,
  Product_Price string,
  Product_price_applicable_start_date timestamp,
  Product_price_applicable_end_date timestamp,
  Product_Price_Currency string,
  Currency_Conversion_source_country_code string,
  Product_Max_Discount_Amount_in_Dollars float,
  Product_Max_Discount_Percentage int,
  src_rec_created_ts timestamp
)
using delta
location '/mnt/jnj/warehouse/demo/Dim_Product_Price_Reference/'

-- COMMAND ----------

create table if not exists demo.Dim_Manufacturer_Detail
(
  Manufacturer_Id string,
  Manufacturer_Name string,
  Manufacturer_Location string
)
using delta
location '/mnt/jnj/warehouse/demo/Dim_Manufacturer_Detail/'

-- COMMAND ----------

create table if not exists demo.Dim_Products
(
  Product_Id string,
  Product_Name string,
  Product_Manufacturer_Id string,
  Product_Availability_Status boolean,
  Product_Base_unit_Price bigint,
  Discount_Applicability boolean
)
using delta 
location '/mnt/jnj/warehouse/demo/Dim_Products/'
partitioned by (Product_Availability_Status,Discount_Applicability)

-- COMMAND ----------

create table if not exists demo.Dim_Customers
(
  Customer_Id string,
  Customer_Name string,
  Customer_DOB string,
  Customer_Contact_number string,
  Customer_Activity_Status boolean,
  Customer_Subscription_Renewal_Flag boolean
)
using delta
location '/mnt/jnj/warehouse/demo/Dim_Customers/'

-- COMMAND ----------


create table if not exists demo.Dim_Vendors
(
  Vendor_Id string,
  Vendor_Org_Name string,
  Vendor_Insured_Id string,
  Vendor_Registration_Id string,
  Vendor_Registration_Date string,
  Vendor_Contact_number string,
  Vendor_Activity_Status boolean,
  src_rec_created_ts timestamp
)
using delta
location '/mnt/jnj/warehouse/demo/Dim_Vendors/'

-- COMMAND ----------

create table if not exists demo.Fact_Transactions
(
  Transaction_Id string,
  Customer_Id string,
  Product_Id string,
  Manufacturer_Id string,
  Units_Ordered bigint,
  Product_Price double,
  Discount_Amount_Per_Unit double,
  Total_Discount_Amount float,
  Final_Amount bigint
)
using delta
location '/mnt/jnj/warehouse/demo/Fact_Transactions/'

-- COMMAND ----------


