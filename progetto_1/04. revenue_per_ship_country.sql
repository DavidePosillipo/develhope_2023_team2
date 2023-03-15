SELECT
   o.shipCountry AS country,
   ROUND(sum(od.UnitPrice * od.Quantity*(1-od.discount)), 2) AS country_revenue
FROM
   Orders o
      LEFT JOIN "Order Details" od ON o.OrderID =od.OrderID 
GROUP BY 
   country
ORDER BY
   country_revenue DESC
;