SELECT 
	e.LastName || ' ' || e.FirstName AS 'Employee',
	STRFTIME('%Y', o.OrderDate) as Year,
	STRFTIME('%m', o.OrderDate) as Month,
	CAST(ROUND(sum(od.UnitPrice*od.Quantity*(1-od.Discount)), 2) AS INT) AS'sales_volume',
	count(*) AS 'num_of_sales'
FROM   
	Employees e 
LEFT JOIN
	Orders o ON e.EmployeeID = o.EmployeeID 
LEFT JOIN 
	"Order Details" od ON o.OrderID = od.OrderID 
GROUP BY 
	employee, Year, Month 
ORDER BY 
	o.OrderDate
;