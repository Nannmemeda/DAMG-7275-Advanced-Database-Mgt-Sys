
-- DAMG 7275 Lab 5c (11 points)
/*
The recursive code below creates a hierarchy graph, as displayed below.
*/
USE Graph;

WITH DirectReports
AS
 (
  -- Anchor member definition
  SELECT
ReportsTo
,Department
, EmployeeID
, LastName
, 0 AS [Level]
    , CONVERT(NVARCHAR(20), NULL) AS MgrName
  FROM OrgHierarchy e
  WHERE ReportsTo IS NULL
  UNION ALL
   --Recursive member definition
  SELECT
   E.ReportsTo
   ,E.Department
   , E.EmployeeID
   , E.LastName
   , [Level] + 1
   , (SELECT LastName 
   FROM OrgHierarchy Emp
   WHERE Emp.EmployeeID = E.ReportsTo) AS MgrName
  FROM OrgHierarchy E INNER JOIN DirectReports DR
   ON E.ReportsTo = DR.EmployeeID
 )
-- Statement that executes the CTE
SELECT EmployeeID AS EmpID
  ,LastName AS EmpLName
  ,Department
  ,[Level]
  ,ReportsTo AS MgrID
  ,MgrName AS MgrLName
FROM DirectReports
ORDER BY Department, [Level], MgrLName;

/*   
EmpID EmpLName Department Level MgrID MgrLName
2 Fuller NULL 0 NULL NULL
5 Buchanan Finance 1 2 Fuller
6 Suyama Finance 2 5 Buchanan
7 King Finance 2 5 Buchanan
12 Chang Finance 3 7 King
13 Morales Finance 4 12 Chang
14 Ng Finance 4 12 Chang
16 Lee Finance 5 14 Ng
17 Spencer Finance 6 16 Lee
20 White Finance 6 16 Lee
22 Norman Finance 6 16 Lee
19 Smith Finance 7 17 Spencer
3 Leverling IT 1 2 Fuller
1 Davolio IT 2 3 Leverling
4 Peacock IT 2 3 Leverling
8 Callahan IT 3 4 Peacock
9 Dodsworth IT 3 4 Peacock
10 Robinson IT 4 8 Callahan
11 Smith IT 4 8 Callahan
15 Black IT 5 11 Smith
21 Thompson IT 5 11 Smith
*/

/*
Please rewrite the SQL code above using WHILE and SQL CURSOR to create the graph displayed below.
Notes about the returned data:

"Level" is where an employee is located in the organizational hierarchy.
"NumberOfReports" is the number of employees reporting to the employee either directly or indirectly.
"DirectReportNames" is the last names of the employees reporting directly to the mentioned employee.
"IndirectReportNames" is the last names of the employees reporting indirectly to the mentioned employee.

Return only the employees who have more than 5 reports (direct + indirect).
Sort the returned data by the NumberOfReports in descending.
Submit your code.
*/


/*
EmpID EmpLName Department Level ManagerLName NumberOfReports
DirectReportNames IndirectReportNames
2 Fuller      0      20 Buchanan, Leverling Black, Callahan, Chang, Davolio, Dodsworth, King, Lee, Morales, Ng, Norman, Peacock, Robinson, Smith, Smith, Spencer, Suyama, Thompson, White
5 Buchanan Finance 1 Fuller 10  King, Suyama Chang, Lee, Morales, Ng, Norman, Smith, Spencer, White
7 King Finance 2 Buchanan 8 Chang Lee, Morales, Ng, Norman, Smith, Spencer,White
3 Leverling IT 1 Fuller 8   Davolio, Peacock Black, Callahan, Dodsworth, Robinson, Smith, Thompson
12 Chang Finance 3 King 7   Morales, Ng Lee, Norman, Smith, Spencer, White
4 Peacock IT 2 Leverling 6  Callahan, Dodsworth Black, Robinson, Smith, Thompson
*/

WITH DirectReports AS(
	SELECT ReportsTo, Department, EmployeeID, LastName, 0 AS Level, CONVERT(NVARCHAR(20), NULL) AS ManagerLName
	FROM OrgHierarchy e
	WHERE ReportsTo IS NULL
	UNION ALL
	--Recursive member definition
	SELECT e.ReportsTo, e.Department, e.EmployeeID, e.LastName, Level + 1, 
		   (SELECT LastName FROM OrgHierarchy Emp WHERE Emp.EmployeeID = E.ReportsTo) AS ManagerLName
	FROM OrgHierarchy e INNER JOIN DirectReports dr
	ON e.ReportsTo = dr.EmployeeID),
Reports AS (
    SELECT o1.EmployeeID,
	   STUFF((SELECT  ', '+RTRIM(CAST(o2.EmployeeID as char))  
       		  FROM OrgHierarchy o2
       		  WHERE o1.EmployeeID = o2.ReportsTo
       		  ORDER BY o2.LastName
       		  FOR XML PATH('')) , 1, 2, '') AS DirectReports
FROM OrgHierarchy o1)
SELECT TOP 100 dr.EmployeeID AS EmpID, LastName AS EmpLName, Department, Level, ReportsTo, ManagerLName AS ManagerLName, DirectReports
INTO #tempReports
FROM DirectReports dr
INNER JOIN Reports rs
ON dr.EmployeeID = rs.EmployeeID
ORDER BY Level Desc;

DECLARE @DirectReports varchar(1000) = '';
DECLARE @empid int;
DECLARE @ReportsTo int;
DECLARE @DirectReports2 varchar(1000) = '';
DECLARE @IndirectReports2 varchar(1000) = '';
DECLARE @empid2 int;
DECLARE @level2 int;
DECLARE @EmpLName2 varchar(1000);
DECLARE @ManagerLName2 varchar(1000);
DECLARE @Department2 varchar(1000);
DECLARE @ReportsTo2 int;

CREATE TABLE #temp
(EmpID int,
 EmpLName varchar(1000),
 Department varchar(1000),
 Level int,
 ReportsTo int,
 ManagerLName varchar(1000),
 DirectReports varchar(1000),
 IndirectReports varchar(1000));

DECLARE manager_cursor CURSOR FOR
SELECT EmpID, ReportsTo, DirectReports
FROM #tempReports
WHERE DirectReports IS NOT NULL;

OPEN manager_cursor;
FETCH NEXT FROM manager_cursor INTO @empid, @ReportsTo, @DirectReports;

WHILE @@FETCH_STATUS = 0
BEGIN

    SELECT @IndirectReports2 = @DirectReports + ','
    FROM #tempReports dr
    WHERE dr.EmpID = @ReportsTo;

	
	IF EXISTS (SELECT IndirectReports FROM #temp where EmpID = @ReportsTo)
    	SELECT @IndirectReports2 = @IndirectReports2 + @DirectReports + ',' + tp.IndirectReports + ','
    	FROM #temp tp
    	WHERE tp.EmpID = @ReportsTo;
   
    IF EXISTS (SELECT IndirectReports FROM #temp where EmpID = @empid)
    	SELECT @IndirectReports2 = @IndirectReports2 + ',' + tp.IndirectReports + ','
    	FROM #temp tp
    	WHERE tp.EmpID = @empid;
   
    SELECT @empid2 = dr.EmpID, @EmpLName2 = dr.EmpLName, @Department2 = dr.Department, @level2 = dr.Level, @ReportsTo2 = dr.ReportsTo, 
   		  @ManagerLName2 = dr.ManagerLName, @DirectReports2 = dr.DirectReports
    FROM #tempReports dr
    WHERE dr.EmpID = @ReportsTo;
   
   
    IF EXISTS (SELECT 1 FROM #temp where EmpID = @empid2)
    	UPDATE #temp
    	SET IndirectReports = @IndirectReports2
    	WHERE EmpID = @empid2
    ELSE 
    	INSERT INTO #temp
    	VALUES (@empid2, @EmpLName2, @Department2, @level2, @ReportsTo2, @ManagerLName2, @DirectReports2, @IndirectReports2);

    FETCH NEXT FROM manager_cursor INTO @empid, @ReportsTo, @DirectReports;
END
CLOSE manager_cursor;   
DEALLOCATE manager_cursor; 

WITH t1 AS (
	SELECT indirectReport, EmpID, LastName
	FROM (
		SELECT distinct value AS indirectReport, EmpID 
		FROM #temp
		CROSS APPLY STRING_SPLIT(IndirectReports, ',')
		WHERE value != '') z1
	INNER JOIN OrgHierarchy
	ON z1.indirectReport = EmployeeID),
t2 AS (
	SELECT directReport, EmpID, LastName
	FROM (
		SELECT distinct value AS directReport, EmpID 
		FROM #temp
		CROSS APPLY STRING_SPLIT(DirectReports, ',')
		WHERE value != '') z2
	INNER JOIN OrgHierarchy
	ON z2.directReport = EmployeeID),
t3 AS (
	SELECT EmpID, COUNT(indirectReport) AS indirectReportNum
	FROM t1 
	GROUP BY EmpID),
t4 AS (
	SELECT EmpID, COUNT(directReport) AS directReportNum
	FROM t2 
	GROUP BY EmpID),
full_reports AS (
	SELECT o1.EmployeeID,
		   STUFF((SELECT  ', '+RTRIM(CAST(t2.LastName as char))  
       			  FROM t2
       			  WHERE o1.EmployeeID = t2.EmpID
       			  ORDER BY t2.LastName
       			  FOR XML PATH('')) , 1, 2, '') AS DirectReportNames,
	   	   STUFF((SELECT  ', '+RTRIM(CAST(t1.LastName as char))  
       			  FROM t1
       		 	  WHERE o1.EmployeeID = t1.EmpID
       		   	  ORDER BY t1.LastName
       		      FOR XML PATH('')) , 1, 2, '') AS IndirectReportNames
			      FROM OrgHierarchy o1)
SELECT t.EmpID, t.EmpLName, t.Department, t.[Level], t.ManagerLName, t3.indirectReportNum + t4.directReportNum AS NumberOfReports, f.DirectReportNames, f.IndirectReportNames
FROM #temp t
INNER JOIN full_reports f ON t.EmpID = f.EmployeeID
INNER JOIN t3 ON t.EmpID = t3.EmpID
INNER JOIN t4 ON t.EmpID = t4.EmpID
WHERE t3.indirectReportNum + t4.directReportNum > 5
ORDER BY (t3.indirectReportNum + t4.directReportNum) DESC;

DROP TABLE #temp;
DROP TABLE #tempReports;