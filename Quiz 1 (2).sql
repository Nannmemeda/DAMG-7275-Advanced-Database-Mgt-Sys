
/* We need to store the columns listed below in a Cassandra Datastax
 Astra database. Our data usage pattern is:

 (1) Get the highest order value for each territory by the territory id.
 Include TerritoryID, TerritoryName, OrderValue in the returned data.
 (2) Retrieve all orders of a customer by the customer id and display the
 returned order(s) in the descending order of the order date.
Include CustomerID, CustomerName, SalesOrderID, OrderDate, OrderValue
in the returned data.

* Use the CustomerID 12345 as an example for this query.

Here are the question(s):
 (1) Design the database and write CQL code to implement it.

 (2) Write the two CQL queries to best support the above-mentioned
 data usage pattern.
For the best performance, "ALLOW FILTERING" and "User-defied Index"
are not allowed.
 Please submit:
 1) CQL code for creating the database (table creation only)
 based on the above requirements (4 points)
 2) Two CQL queries based on the above requirements (3 points)
 You don't have to enter data in the database, but for testing
 your work, you may want to enter some data. 

 SalesOrderID
 OrderDate
 OrderValue
 CustomerID
 CustomerName
 TerritoryID
 TerritoryName  */


/* Question 1 */

CREATE TABLE demo.SalesInfo(
    SalesOrderID int,
    OrderDate date,
    OrderValue int,
    CustomerID int,
    CustomerName text,
    TerritoryID int,
    TerritoryName text,
    PRIMARY KEY (TerritoryID, OrderValue)
);

INSERT INTO demo.SalesInfo(SalesOrderID, OrderDate, OrderValue, CustomerID, CustomerName,TerritoryID, TerritoryName)
VALUES(123, '2017-01-01',500,1257,'Nancy',1,'Asia');

INSERT INTO demo.SalesInfo(SalesOrderID, OrderDate, OrderValue, CustomerID, CustomerName, TerritoryID, TerritoryName)
VALUES(124, '2017-01-02',600,1180,'Kaicheng',2,'America');

INSERT INTO demo.SalesInfo(SalesOrderID, OrderDate, OrderValue, CustomerID, CustomerName, TerritoryID, TerritoryName)
VALUES(125, '2017-01-03',400,1280,'Kevin',1,'Asia');

INSERT INTO demo.SalesInfo(SalesOrderID, OrderDate, OrderValue, CustomerID, CustomerName, TerritoryID, TerritoryName)
VALUES(126, '2017-01-04',800,1259,'Fish',2,'America');

SELECT TerritoryID, TerritoryName, MAX(OrderValue)
FROM demo.SalesInfo
GROUP BY TerritoryID;

/* Question 2 */

CREATE TABLE demo.Sales(
    SalesOrderID int,
    OrderDate date,
    OrderValue int,
    CustomerID int,
    CustomerName text,
    TerritoryID int,
    TerritoryName text,
    PRIMARY KEY (CustomerID, OrderDate)
);

INSERT INTO demo.Sales(SalesOrderID, OrderDate, OrderValue, CustomerID, CustomerName,TerritoryID, TerritoryName)
VALUES(1234, '2019-01-01',500,12344,'Kaicheng',1,'Asia');

INSERT INTO demo.Sales(SalesOrderID, OrderDate, OrderValue, CustomerID, CustomerName,TerritoryID, TerritoryName)
VALUES(1235, '2019-01-01',1000,12345,'Nancy',1,'Asia');

INSERT INTO demo.Sales(SalesOrderID, OrderDate, OrderValue, CustomerID, CustomerName,TerritoryID, TerritoryName)
VALUES(1236, '2019-01-02',400,12345,'Nancy',1,'Asia');

INSERT INTO demo.Sales(SalesOrderID, OrderDate, OrderValue, CustomerID, CustomerName,TerritoryID, TerritoryName)
VALUES(1237, '2019-01-03',500,12345,'Nancy',1,'Asia');

SELECT CustomerID, CustomerName, SalesOrderID, OrderDate, OrderValue
FROM demo.Sales
WHERE CustomerID = 12345
ORDER BY OrderDate DESC;





USE AdventureWorks2017;


/* Cosmos DB SQL API - 8 points */
-- Part 1 (5 points)
/*
Using "FOR JSON PATH" and AdventureWorks2017, write a SQL query to retrieve
all salesperson(s) for each sales territory and a salesperson's top 3 orders
in the territory.
The top 3 orders have the 3 highest TotalDue amounts. The TotalDue amount is
in SalesOrderHeader. If there is a tie, the tie must be retrieved.
Return the data in the JSON format as displayed below.
The bonus is in the SalesPerson table.
Please use the format just for formatting purposes. It doesn't include all
required data.
Submit the SQL code.
*/

WITH temp AS (
    SELECT soh.TerritoryID, st.Name, p.BusinessEntityID, CAST(sp.Bonus AS INT) Bonus, p.LastName, p.FirstName,
        soh.SalesOrderID, CAST(soh.TotalDue AS INT) AS 'OrderAmount',
        RANK() OVER (PARTITION BY p.BusinessEntityID, soh.TerritoryID ORDER BY soh.Totaldue DESC) rank
    FROM Sales.SalesOrderHeader soh
    JOIN Sales.SalesTerritory st
    ON soh.TerritoryID = st.TerritoryID
    JOIN Person.Person p
    ON soh.SalesPersonID = p.BusinessEntityID
    JOIN Sales.SalesPerson sp
    ON sp.BusinessEntityID = p.BusinessEntityID
),
after_rank AS (
    SELECT TerritoryID, Name, BusinessEntityID, Bonus, LastName, FirstName,SalesOrderID, OrderAmount
    FROM temp
    WHERE rank <= 3
)

SELECT DISTINCT ar3.TerritoryID, ar3.Name, 
    (SELECT DISTINCT ar2.BusinessEntityID, ar2.Bonus, ar2.LastName, ar2.FirstName, 
        (SELECT ar1.SalesOrderID, ar1.OrderAmount
        FROM after_rank ar1
        WHERE ar1.BusinessEntityID = ar2.BusinessEntityID AND ar1.TerritoryID = ar3.TerritoryID
        ORDER BY ar1.OrderAmount DESC
        FOR JSON PATH) Top3Orders
    FROM after_rank ar2
    WHERE ar2.TerritoryID = ar3.TerritoryID
    ORDER BY BusinessEntityID
    FOR JSON PATH) SalesPersons
FROM after_rank ar3
ORDER BY ar3.TerritoryID
FOR JSON PATH;


/*
[{"TerritoryID":1,
 "Name":"Northwest",
 "SalesPersons":[{"BusinessEntityID":280,
 "Bonus":5000,
 "LastName":"Ansman-Wolfe",
 "FirstName":"Pamela",
 "Top3Orders":[{"SalesOrderID":47033,"OrderAmount":105494},
 {"SalesOrderID":67297,"OrderAmount":103227},
 {"SalesOrderID":53518,"OrderAmount":99024}]},
 {"BusinessEntityID":283,
 "Bonus":3500,
 "LastName":"Campbell",
 "FirstName":"David",
 "Top3Orders":[{"SalesOrderID":46643,"OrderAmount":123497},
 {"SalesOrderID":51711,"OrderAmount":114537},
 {"SalesOrderID":51123,"OrderAmount":105282}]},
 {"BusinessEntityID":284,
 "Bonus":3900,
 "LastName":"Mensa-Annan",
 "FirstName":"Tete",
 "Top3Orders":[{"SalesOrderID":69508,"OrderAmount":119641},
 {"SalesOrderID":50297,"OrderAmount":116391},
 {"SalesOrderID":48057,"OrderAmount":97614}]}]},
{"TerritoryID":2,
 "Name":"Northeast",
 "SalesPersons":[{"BusinessEntityID":275,
 "Bonus":4100,
 "LastName":"Blythe",
 "FirstName":"Michael",
 "Top3Orders":[{"SalesOrderID":47395,"OrderAmount":165029},
 {"SalesOrderID":48336,"OrderAmount":113231},
 {"SalesOrderID":46666,"OrderAmount":99952}]}]}
**************** There is more data which is not displayed here ***************
*/


-- Part 3 (2 points)
/*
 Write a SQL query for the Cosmos DB SQL API to get
 the total value of the top 3 orders for each salesperson
 in each territory. Include only the salespersons who have a
 BusinessEntityID > 285.
 Return the territory id, salesperson's last and first names,
 and the total value.
 Submit the code and a screenshot of execution results.
*/

SELECT t.TerritoryID, s.LastName, s.FirstName, SUM(m.OrderAmount) AS TotalAmount
FROM t
JOIN s IN t.SalesPersons
JOIN m IN s.Top3Orders
WHERE s.BusinessEntityID > 285
GROUP BY t.TerritoryID, s.LastName, s.FirstName