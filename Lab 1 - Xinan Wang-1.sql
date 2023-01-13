
/* DAMG 7275 Lab 1 -- Xinan Wang */

/* Question 1: Use AdventureWorks2017, please write SQL code to create the report displayed in the attached
file. */

USE AdventureWorks2017;

WITH SourceTable AS (
	SELECT YEAR(soh.OrderDate) AS OrderYear, sod.ProductID AS ProductID, SUM(sod.OrderQty * sod.UnitPrice) AS [YearSale]
	FROM Sales.SalesOrderDetail sod 
	JOIN Sales.SalesOrderHeader soh 
	ON sod.SalesOrderID = soh.SalesOrderID
	GROUP BY YEAR(soh.OrderDate), sod.ProductID),
YearlyTotal AS (
	SELECT YEAR(OrderDate) AS OrderYear,
		SUM(TotalDue) AS AnnualTotal  
	FROM Sales.SalesOrderHeader
	GROUP BY YEAR(OrderDate)),
PivotTable AS (SELECT OrderYear,[715],[716],[717]
	FROM SourceTable
	PIVOT (MAX(YearSale) FOR ProductID IN ([715],[716],[717])) AS PivotTable)

SELECT p.OrderYear, ROUND([715],0) AS [715], CAST(ROUND((100 * [715]/ y.AnnualTotal),2) AS VARCHAR) + '%' AS '% of Total',
	RIGHT('          ' + 
		CAST(ISNULL(FORMAT([715] - LAG([715],1,NULL) OVER(ORDER BY p.OrderYear),'N0'),' ') AS VARCHAR), 100) AS YearlyChange,
	ROUND([716],0) AS [716], CAST(ROUND((100 * [716]/ y.AnnualTotal),2) AS VARCHAR) + '%' AS '% of Total',
	RIGHT('          ' + 
		CAST(ISNULL(FORMAT([716] - LAG([716],1,NULL) OVER(ORDER BY p.OrderYear),'N0'),' ') AS VARCHAR), 100) AS YearlyChange,
	ROUND([717],0) AS [717], CAST(ROUND((100 * [717]/ y.AnnualTotal),2) AS VARCHAR) + '%' AS '% of Total',
	RIGHT('          ' + 
		CAST(ISNULL(FORMAT([717] - LAG([717],1,NULL) OVER(ORDER BY p.OrderYear),'N0'),' ') AS VARCHAR), 100) AS YearlyChange,
	ROUND(AnnualTotal,0) AS TotalAnnualSale,
	RIGHT('          ' + 
		ISNULL(CAST(ROUND(100 * ((ROUND(AnnualTotal,0) / LAG(ROUND(AnnualTotal,0),1,NULL) OVER(ORDER BY p.OrderYear)) - 1), 2) AS VARCHAR) + '%', ' '),50) AS 'Annual %',
	RIGHT('          ' + 
		CAST(ISNULL(FORMAT(ROUND(AnnualTotal,0) - LAG(ROUND(AnnualTotal,0),1,NULL) OVER(ORDER BY p.OrderYear),'N0'),' ') AS VARCHAR), 50) AS YearlyChange
FROM PivotTable p
JOIN YearlyTotal y 
ON p.OrderYear = y.OrderYear


/* Question 2: Write a query to retrieve the least valuable salesperson of each city.
The least valuable salesperson has the smallest total sales amount 
in a city.
Use TotalDue in SalesOrderHeader for calculating the totalsales amount.
Use ShipToAddressID in SalesOrderHeader to determine what city 
an order is related to. Exclude orders which don't have a salesperson
specified.
Return only the salesperson(s) who has sold more than $600000 in 
the same city and has done business in more than one sales territory.
If there is a tie, your solution must retrieve it.
Include City, SalesPersonID, salesperson's last and first names,
and Total sales of the least valuable salesperson in the same city
for the returned data. Sort the returned data by City. */

/* The original code */

WITH temp AS
(SELECT SalesPersonID ,City,
SUM(TotalDue) "Total Sales",
RANK() OVER (PARTITION BY City ORDER BY SUM(TotalDue)) "Ranking"
FROM Sales.SalesOrderHeader soh
JOIN Person.Address a
ON a.AddressID = soh.ShipToAddressID
WHERE soh.SalesPersonID IN (
SELECT SalesPersonID
FROM Sales.SalesOrderHeader
WHERE SalesPersonID IS NOT NULL
GROUP BY SalesPersonID
HAVING COUNT(DISTINCT TerritoryID) > 1)
GROUP BY SalesPersonID ,City
HAVING SUM(TotalDue) > 600000
)
SELECT City ,SalesPersonID ,LastName ,FirstName ,"Total Sales"
FROM Person.Person p
JOIN temp
ON p.BusinessEntityID = temp.SalesPersonID
WHERE Ranking = 1
ORDER BY City;

/* The order of the selecting is totally wrong. We should find the least valuable sale person of each city 
 * first, and then find the salesperson who has sold more than 600000 in the same city and has done business
 * in more than 1 sales territory and then select these data. */

/* The corrected code */

WITH temp AS (
	SELECT SalesPersonID, City, SUM(TotalDue) AS [Total Sales],
		RANK() OVER (PARTITION BY City ORDER BY SUM(TotalDue)) AS Ranking 
	FROM Sales.SalesOrderHeader soh 
	JOIN Person.Address a
	ON a.AddressID = soh.ShipToAddressID
	GROUP BY SalesPersonID, City),
conditionperson AS (
	SELECT SalesPersonID
	FROM Sales.SalesOrderHeader
	WHERE SalesPersonID IS NOT NULL
	GROUP BY SalesPersonID
	HAVING COUNT(DISTINCT TerritoryID) > 1)
	
SELECT City, SalesPersonID, LastName, FirstName, [Total Sales]
FROM Person.Person p
JOIN temp t 
ON p.BusinessEntityID = t.SalesPersonID 
WHERE RANKING = 1 AND [Total Sales] > 600000 AND SalesPersonID IN (SELECT SalesPersonID FROM conditionperson)
ORDER BY City;
