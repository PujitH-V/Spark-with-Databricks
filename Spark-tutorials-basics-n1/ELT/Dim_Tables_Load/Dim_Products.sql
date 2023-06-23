-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Execute the Dim_Manufacturer_Detail notebook first to make the data available to use in the current notebook.

-- COMMAND ----------

-- MAGIC %run ./Dim_Manufacturer_Detail

-- COMMAND ----------

create or replace temp view src_products as
select 
DP.Product_Id,
DP.Product_Name,
DMD.Manufacturer_Name,
case when (DP.Product_Availability_Status = 'Available') then true
else false
end as Availability_Status,
DP.Product_Base_unit_Price,
DP.Discount_Applicability,
DP.Record_Created_timestamp
from global_temp.demo_Products DP
left join demo.Dim_Manufacturer_Detail DMD 
on DP.Product_Manufacturer_Id = DMD.Manufacturer_Id

-- COMMAND ----------

MERGE INTO demo.Dim_Products T
USING src_products S
ON T.Product_Id = S.Product_Id

WHEN MATCHED THEN
UPDATE SET 
T.Product_Name = S.Product_Name,
T.Product_Manufacturer_Id = S.Manufacturer_Name,
T.Product_Availability_Status = S.Availability_Status,
T.Product_Base_unit_Price = S.Product_Base_unit_Price,
T.Discount_Applicability = S.Discount_Applicability,
T.src_record_created_timestamp = S.Record_Created_timestamp

WHEN NOT MATCHED THEN
INSERT
(
  Product_Id,Product_Name,Product_Manufacturer_Id,Product_Availability_Status,Product_Base_unit_Price,Discount_Applicability,src_record_created_timestamp
)
VALUES
(Product_Id,Product_Name,Manufacturer_Name,Availability_Status,Product_Base_unit_Price,Discount_Applicability,Record_Created_timestamp)

-- COMMAND ----------

select * from demo.Dim_Products

-- COMMAND ----------


