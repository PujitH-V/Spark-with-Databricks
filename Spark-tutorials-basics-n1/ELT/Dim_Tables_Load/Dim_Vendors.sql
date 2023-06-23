-- Databricks notebook source
Merge into demo.Dim_Vendors AS T 
USING global_temp.demo_Vendors AS S 
ON T.Vendor_Id = S.Vendor_Id

WHEN MATCHED and S.Vendor_Activity_Status != 'Active' THEN DELETE

WHEN MATCHED THEN
update set T.Vendor_Org_Name = S.Vendor_Org_Name,
T.Vendor_Insured_Id = S.Vendor_Insured_Id,
T.Vendor_Registration_Id = S.Vendor_Registration_Id,
T.Vendor_Registration_Date = S.Vendor_Registration_Date,
T.Vendor_Contact_number = S.Vendor_Contact_number,
T.Vendor_Activity_Status = S.Vendor_Activity_Status,
T.src_rec_created_ts = S.Record_Created_timestamp

WHEN NOT MATCHED THEN 
INSERT (Vendor_Id,
Vendor_Org_Name,
Vendor_Insured_Id,
Vendor_Registration_Id,
Vendor_Registration_Date,
Vendor_Contact_number,
Vendor_Activity_Status,
src_rec_created_ts)
VALUES
(Vendor_Id,
Vendor_Org_Name,
Vendor_Insured_Id,
Vendor_Registration_Id,
Vendor_Registration_Date,
Vendor_Contact_number,
Vendor_Activity_Status,
Record_Created_timestamp)

-- COMMAND ----------


