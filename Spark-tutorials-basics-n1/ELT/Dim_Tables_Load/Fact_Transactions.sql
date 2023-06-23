-- Databricks notebook source
insert into demo.Fact_Transactions 
select Transaction_Id,
Customer_Id,
Product_Id,
Manufacturer_Id,
Units_Ordered,
Product_Price,
Discount_Amount_Per_Unit,
(Units_Ordered*Discount_Amount_Per_Unit) as Total_Discount_Amount,
(Sub_Total - (Units_Ordered*Discount_Amount_Per_Unit)) as Final_Amount
from (select 
TR.Transaction_Id,
TR.Customer_Id,
TR.Product_Id,
MD.Manufacturer_Id,
TR.Units_Ordered,
case when PPRL.Product_Price is not null then PPRL.Product_Price else P.Product_Base_unit_Price end as Product_Price,
case when PPRL.Product_Price is not null then (TR.Units_Ordered*PPRL.Product_Price) 
else (TR.Units_Ordered*P.Product_Base_unit_Price) end as Sub_Total,
case 
    when TR.Transaction_Date BETWEEN PPRL.Product_price_applicable_start_date and PPRL.Product_price_applicable_end_date
    then ((TR.Discount_Percent_Applied/100)*PPRL.Product_Max_Discount_Amount_in_Dollars) 
    else 0
end as Discount_Amount_Per_Unit
from global_temp.demo_Transactions_Reference TR
left join demo.Dim_Customers C on TR.Customer_Id = C.Customer_Id
left join demo.Dim_Products P on Tr.Product_Id = P.Product_Id
left join demo.Dim_Manufacturer_Detail MD on P.Product_Manufacturer_Id = MD.Manufacturer_Id
left join demo.Dim_Product_Price_Reference PPRL on TR.Product_Id = PPRL.Ref_Product_Id) R

-- COMMAND ----------


