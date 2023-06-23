-- Databricks notebook source
Merge into demo.Dim_Customers T 
USING 
(
  select c.Customer_Id,
c.Customer_Name,
cast(c.Customer_DOB as string) as Customer_DOB,
    c.Customer_Contact_number,
    case when (c.Customer_Activity_Status = 'Yes') then true else false end as Customer_Activity_Status,
    c.Customer_Subscription_Renewal_Flag,
    d.Addr_Line1,
    d.Addr_Line2,
    d.City,
    d.Zipcode,
    d.State_info,
    d.Country,
    CASE 
        WHEN (MONTH(current_date()) > MONTH(c.Customer_DOB)) 
            OR (MONTH(current_date()) = MONTH(c.Customer_DOB) AND DAY(current_date()) >= DAY(c.Customer_DOB))
        THEN YEAR(current_date()) - YEAR(c.Customer_DOB)
        ELSE (YEAR(current_date()) - YEAR(c.Customer_DOB)) - 1
    END as Age
from global_temp.demo_customers c
left join global_temp.demo_All_Address_Details d 
on c.Customer_Id = d.F_Customer_Id
where d.primary_address_flag = True
)
 S
ON T.Customer_Id = S.Customer_Id
when MATCHED 
THEN
update set 
T.Customer_Name = S.Customer_Name,
T.Customer_DOB = S.Customer_DOB,
T.Customer_Contact_number = S.Customer_Contact_number,
T.Customer_Activity_Status = S.Customer_Activity_Status,
T.Customer_Subscription_Renewal_Flag = S.Customer_Subscription_Renewal_Flag,
T.Addr_Line1 = S.Addr_Line1,
T.Addr_Line2 = S.Addr_Line2,
T.City = S.City,
T.Zipcode = S.Zipcode,
T.State = S.State_info,
T.Country = S.Country,
T.Age = S.Age

when not matched then
insert 
(Customer_Id,Customer_Name,Customer_DOB,Customer_Contact_number,Customer_Activity_Status,Customer_Subscription_Renewal_Flag,Addr_Line1,Addr_Line2,City,Zipcode,State,Country,Age)
values (Customer_Id,Customer_Name,Customer_DOB,Customer_Contact_number,Customer_Activity_Status,Customer_Subscription_Renewal_Flag,Addr_Line1,Addr_Line2,City,Zipcode,State_info,Country,Age)

-- COMMAND ----------


