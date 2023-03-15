WITH amount_tb AS (
   SELECT
      sum(UnitPrice * Quantity*(1-discount)) as amount,
      o.OrderID,
      o.OrderDate
   FROM 
      Orders o 
         LEFT JOIN "Order Details" od on o.OrderID =od.OrderID
   WHERE 
      STRFTIME('%Y',ShippedDate) in ('2016','2017','2018')
   GROUP BY 
      o.OrderID
)
SELECT DISTINCT 
   amount,
   OrderDate,
   CASE WHEN amount>=1591.25 then 'high'
        WHEN (amount<1591.25 and amount>625.275) then 'medium'
        ELSE 'low' END 'category'
FROM 
   amount_tb
;
