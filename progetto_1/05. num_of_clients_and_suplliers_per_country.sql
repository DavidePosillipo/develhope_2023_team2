-- Number of clients per country
SELECT
   ShipCountry,
   count(DISTINCT CustomerID) AS 'Number of customers'
FROM
   Orders
GROUP BY
   ShipCountry
;

-- Number of suppliers per country
SELECT DISTINCT
   Country AS supply_coutry, 
   count(*) AS 'Number of customers'
FROM 
   Suppliers s 
GROUP BY 
   supply_coutry
;