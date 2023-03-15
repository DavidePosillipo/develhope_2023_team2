SELECT DISTINCT
	c.CategoryName 
FROM
	"Order Details" od
		LEFT JOIN Products p ON p.ProductID = od.ProductID 
			LEFT JOIN Categories c ON c.CategoryID = p.CategoryID 
;