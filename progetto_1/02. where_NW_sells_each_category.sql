WITH country_category_pair AS (
   SELECT DISTINCT 
      o.ShipCountry AS country,
      c.CategoryName AS category
FROM Orders o 
   LEFT JOIN "Order Details" od ON o.OrderID = od.OrderID 
      LEFT JOIN Products p ON od.ProductID = p.ProductID 
         LEFT JOIN Categories c ON p.CategoryID =c.CategoryID
)
SELECT
   cl.country AS country,
   CASE WHEN EXISTS(SELECT TRUE FROM country_category_pair ccp WHERE ccp.country = cl.country AND ccp.category = 'Beverages') THEN TRUE
      ELSE FALSE END AS 'Beverages',
   CASE WHEN EXISTS(SELECT TRUE FROM country_category_pair ccp WHERE ccp.country = cl.country AND ccp.category = 'Condiments') THEN TRUE
      ELSE FALSE END AS 'Condiments',
   CASE WHEN EXISTS(SELECT TRUE FROM country_category_pair ccp WHERE ccp.country = cl.country AND ccp.category = 'Confections') THEN TRUE
      ELSE FALSE END AS 'Confections',
   CASE WHEN EXISTS(SELECT TRUE FROM country_category_pair ccp WHERE ccp.country = cl.country AND ccp.category = 'Dairy Products') THEN TRUE
      ELSE FALSE END AS 'Dairy Products',
   CASE WHEN EXISTS(SELECT TRUE FROM country_category_pair ccp WHERE ccp.country = cl.country AND ccp.category = 'Grains/Cereals') THEN TRUE
      ELSE FALSE END AS 'Grains/Cereals',
   CASE WHEN EXISTS(SELECT TRUE FROM country_category_pair ccp WHERE ccp.country = cl.country AND ccp.category = 'Meat/Poultry') THEN TRUE
      ELSE FALSE END AS 'Meat/Poultry',
   CASE WHEN EXISTS(SELECT TRUE FROM country_category_pair ccp WHERE ccp.country = cl.country AND ccp.category = 'Seafood') THEN TRUE
      ELSE FALSE END AS 'Produce',
   CASE WHEN EXISTS(SELECT TRUE FROM country_category_pair ccp WHERE ccp.country = cl.country AND ccp.category = 'Beverages') THEN TRUE
      ELSE FALSE END AS 'Seafood'
FROM
(SELECT DISTINCT o.ShipCountry AS country FROM Orders o ORDER BY country) cl --County list
;
