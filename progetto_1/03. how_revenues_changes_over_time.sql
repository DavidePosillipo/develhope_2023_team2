SELECT
   STRFTIME('%Y', o.OrderDate) AS Year,
   STRFTIME('%m', o.OrderDate) AS Month,
   o.OrderID AS 'order_id',
   ROUND(sum(od.UnitPrice*od.Quantity*(1-od.Discount)), 2) AS 'sales_volume'
FROM
   Orders o
      LEFT JOIN "Order Details" od ON o.OrderID = od.OrderID
GROUP BY
   Year, Month
;