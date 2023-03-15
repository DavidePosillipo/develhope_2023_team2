SELECT
   e.LastName || ' ' || e.FirstName  AS Employee,
   e.Title AS employee_Title,
   CASE WHEN e.ReportsTo = s.EmployeeID THEN e.LastName || ' ' || e.FirstName
        ELSE  '--'  END AS Supervisor,
   e.Country AS Country
FROM
   Employees e 
      LEFT JOIN Employees s ON e.ReportsTo = s.EmployeeID
ORDER BY 
   e.Country DESC, e.FirstName
;
