SELECT 
	c.Categoryname,
	STRFTIME("%Y",o.OrderDate) as Year,
	sum(od.Quantity*od.UnitPrice*(1-od.Discount)) as Order_value
from 
	"Order Details" od 
	left join Orders o 
	left join Products p on p.ProductID =od.ProductID 
	left join Categories c on c.CategoryID = p.CategoryID 
group BY 
	c.CategoryName, Year
