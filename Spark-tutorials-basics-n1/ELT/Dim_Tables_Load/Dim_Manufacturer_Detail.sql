-- Databricks notebook source
Merge into demo.Dim_Manufacturer_Detail TD
USING global_temp.demo_Manufacturer_Detail SD
ON TD.Manufacturer_Id = SD.Manufacturer_Id

WHEN MATCHED THEN
UPDATE SET
TD.Manufacturer_Name = SD.Manufacturer_Name,
TD.Manufacturer_Location = SD.Manufacturer_Location

WHEN NOT MATCHED THEN
INSERT (Manufacturer_Id, Manufacturer_Name, Manufacturer_Location)
VALUES (Manufacturer_Id, Manufacturer_Name, Manufacturer_Location)

-- COMMAND ----------


