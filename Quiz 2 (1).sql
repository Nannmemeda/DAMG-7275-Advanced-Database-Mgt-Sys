
-- DAMG 7275 Fall 22 Q2 2nd Last Digit of NUID 4, 5, 6, 7, 8 or 9

-- Your Name: Xinan Wang
-- Your NUID: 002916472

-- ArangoDB (7 points)

-- Part 1 (2 points)

/*
Import the provided csv files into ArangoDB to create a graph.
Submit the import code and a screenshot of the importing results.
*/


-- Part 2 (5 points)

/*
Write an AQL query to traverse the imported graph and list all 
reporting chains with 3 on top. Exclude any reporting chain that 
includes 14.

You can use the provided graph image as a reference when traversing 
the graph. The returned data should have a format displayed below.
Use the format for formatting purposes only. The number is the 
employee id. The name is the matching name of the corresponding 
employee id.

Submit the AQL code.
*/

-- ======= This is AQL code ======== --

/* 

FOR v,e,p IN 1..4 INBOUND 'employee/3' workfor
OPTIONS {
bfs: TRUE,
uniqueVertices:'none',
uniqueEdges:'path'
}
FILTER v._key IN ['3','7','12','15','21']
RETURN CONCAT_SEPARATOR('->', p.vertices[*]._id,
    p.vertices[*].lastname)

*/


/*
[
  "[\"employee/3\",\"employee/7\"]->[\"Leverling\",\"King\"]",
  "[\"employee/3\",\"employee/7\",\"employee/12\"]->[\"Leverling\",\"King\",\"Chang\"]",
  "[\"employee/3\",\"employee/7\",\"employee/15\"]->[\"Leverling\",\"King\",\"Black\"]",
  "[\"employee/3\",\"employee/7\",\"employee/15\",\"employee/21\"]->[\"Leverling\",\"King\",\"Black\",\"Thompson\"]"
]
*/



/* Dynamic SQL - 8 points */

/*
Wang's Learning Centers serves under-privileged students nationwide.
It has six service regions. There is a central database which stores 
the current years tutoring sessions, as displayed below. 

create table tutoring(
tutoringid int primary key,
tutoringdate date,
tutoringtime time,
regionid tinyint,
studentid int,
tutorid int,
notes varchar(1000));


In the beginning of a new year,
last year's tutoring sessions are archived to the regional archive tables.

The archive tables have the structure below:

create table tutoringYYYYRR(
tutoringid int primary key,
tutoringdate date,
tutoringtime time,
studentid int,
tutorid int,
notes varchar(1000));

YYYY is the tutoring session's year and RR is the service region ID.
For example, in the beginning of 2019, the 2018's old tutoring sessions
for the service region 03 were archived in an archived table named
tutoring201803, 

Please develop a stored procedure which will take three parameters:

Report beginning year (int)
Report ending year (int)
RegionID (tinyint)

The stored procedure will then return the total number of 
tutoring sessions every quarter for the reporting period.
Please keep in mind, the current year's data may be requested in a report.

The report should have the format displayed below. 

year	quarter     number of tutoring sessions
2019		1			2530
2019		2			3200
2019		3			2955
2019		4			2888
2020		1			2510
2020		2			3522
2020		3			3130
2020		4			2295

*/

USE NancyDB;

create table tutoring(
tutoringid int primary key,
tutoringdate date,
tutoringtime time,
regionid tinyint,
studentid int,
tutorid int,
notes varchar(1000));

create table tutoringYYYYRR(
tutoringid int primary key,
tutoringdate date,
tutoringtime time,
studentid int,
tutorid int,
notes varchar(1000));

CREATE PROC [dbo].[yearReport]
  @BeginYear INT, @EndYear INT, @RegionID TINYINT 
AS
BEGIN

DECLARE @SQLText VARCHAR(MAX);
DECLARE @year INT;
DECLARE @quarter INT;
DECLARE @SumOfSessions INT;

CREATE TABLE #temp (
  year INT,
  quarter INT,
  [number of tutoring sessions] INT
);

DECLARE sql_cursor CURSOR FOR
  SELECT YEAR(tutoringdate) year, QUARTER(tutoringdate) quarter
  FROM tutoring
  WHERE YEAR(tutoringdate) BETWEEN @BeginYear AND @EndYear AND regionid = @RegionID

OPEN sql_cursor;
FETCH NEXT FROM sql_cursor INTO @year, @quarter;

WHILE @@FETCH_STATUS = 0
BEGIN

  SET @SumOfSessions = 0;

  SET @SQLText = 'SELECT COUNT(tutoringid) AS NumSessions
                    FROM tutoring
                    WHERE YEAR(tutoringdate) = ' + @year + 'AND QUARTER(tutoringdate) = ' + @quarter +
                    'GROUP BY YEAR(tutoringdate) AND QUARTER(tutoringdate)';
  SELECT @year, @SumOfSessions = @SumOfSessions + CAST(EXEC(@SQLText) AS INT);
  INSERT INTO #temp VALUES(@year, @quarter, @SumOfSessions);

  FETCH NEXT FROM sql_cursor INTO @year, @quarter;
END

CLOSE sql_cursor;
DEALLOCATE sql_cursor;

SELECT * FROM #temp;
DROP TABLE #temp;

END;


