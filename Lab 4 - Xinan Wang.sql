/* Lab 4 -- Xinan Wang */

/*  Part 1 Use the sample airports (node) and flights (edge) collections for this question. 
 
Please write an AQL query to return all the qualified flight(s) from Seattle to Dallas-Fort Worth. Seattle 
and Dallas-Fort Worth are airport cities. Return only the direct flights and flights with only one layover 
stop. Sort the returned data by the total distance in ascending. 
 
The returned flights must meet the following requirements: 
1) Depart from Seattle after 2150 (departure time) 
2) The day of week for the departure must be 4 
3) The departure flight from Seattle must be an AS (carrier) flight 
4) If there is a layover stop, the second flight may be another carrier’s flight 
5) The total distance must be fewer than 2000 miles 
 
The AQL query should return data in the format below. Please use the format below only for formatting 
purposes. It doesn’t have complete returned data. 
 
Notes about the format: 
a) Don’t worry about getting rid of extra symbols, such as “\” 
b) A returned row starts with 2 or 3 airport cities 
c) After airport cities, there are carrier(s) and flight number(s) 
d) Then the departure time and distance in miles 
e) The distance is between two airport cities 
f) The last number is the total distance from Seattle to Dallas-Fort Worth 
g) Airport cities, distance, and total are separated by “->”  */

FOR v, e, p IN 1..2 OUTBOUND 'airports/SEA' flights
FILTER v._id == 'airports/DFW'
FILTER p.edges[0].DepTime > 2150
FILTER p.edges[*].DayOfWeek ALL == 4
FILTER p.edges[0].UniqueCarrier == 'AS'
FILTER p.edges[0].Distance + p.edges[1].Distance <= 2000
SORT p.edges[0].Distance + p.edges[1].Distance ASC
return concat_separator('->', p.vertices[*].city, 
                        p.edges[*].UniqueCarrier, 
                        'Flight No', p.edges[*].FlightNum,
                        'Departure Time', p.edges[*].DepTime,
                        'Distance', p.edges[*].Distance,
                        'Total', p.edges[0].Distance + p.edges[1].Distance)



/* Part 2 – Cosmos DB Gremlin API (4 points) 
 
We’ll use Cosmos DB (Gremlin API) for Part 2 
 
1) Write Gremlin code to implement the attached employee data and work relationship graph in 
Cosmos DB (Gremlin API).  . */


g.addV('person').property('LastName','Fuller').property('FirstName','Andrew').property('EmployeeID',2)
g.addV('person').property('LastName','Leverling').property('FirstName','Janet').property('EmployeeID',3).Property('Department','IT')
g.addV('person').property('LastName','Buchanan').property('FirstName','Steven').property('EmployeeID',5).Property('Department','Finance')
g.addV('person').property('LastName','King').property('FirstName','Robert').property('EmployeeID',7).Property('Department','Finance')
g.addV('person').property('LastName','Chang').property('FirstName','Leslie').property('EmployeeID',12).Property('Department','Finance')
g.addV('person').property('LastName','Ng').property('FirstName','Jordan').property('EmployeeID',14).Property('Department','Finance')
g.addV('person').property('LastName','Black').property('FirstName','Lela').property('EmployeeID',15).Property('Department','IT')
g.addV('person').property('LastName','Thompson').property('FirstName','Connie').property('EmployeeID',21).Property('Department','IT')


g.V().hasLabel('person').has('EmployeeID',21).addE('reportsto').to(g.V().hasLabel('person').has('EmployeeID',15))
g.V().hasLabel('person').has('EmployeeID',15).addE('reportsto').to(g.V().hasLabel('person').has('EmployeeID',7))
g.V().hasLabel('person').has('EmployeeID',15).addE('reportsto').to(g.V().hasLabel('person').has('EmployeeID',2))
g.V().hasLabel('person').has('EmployeeID',14).addE('reportsto').to(g.V().hasLabel('person').has('EmployeeID',7))
g.V().hasLabel('person').has('EmployeeID',12).addE('reportsto').to(g.V().hasLabel('person').has('EmployeeID',7))
g.V().hasLabel('person').has('EmployeeID',7).addE('reportsto').to(g.V().hasLabel('person').has('EmployeeID',3))
g.V().hasLabel('person').has('EmployeeID',7).addE('reportsto').to(g.V().hasLabel('person').has('EmployeeID',2))
g.V().hasLabel('person').has('EmployeeID',7).addE('reportsto').to(g.V().hasLabel('person').has('EmployeeID',5))
g.V().hasLabel('person').has('EmployeeID',3).addE('reportsto').to(g.V().hasLabel('person').has('EmployeeID',2))
g.V().hasLabel('person').has('EmployeeID',5).addE('reportsto').to(g.V().hasLabel('person').has('EmployeeID',2))


/* 2) Write a Gremlin query to retrieve all employees in the IT department. Return the complete 
data about an employee.  */

g.V().hasLabel(‘person’).has(‘Department’,’IT’)


/* 3) Write a gremlin query to retrieve all employees who have EmpID 2 as either a direct manager 
or an indirect manager. Return just the employee ids. */


g.V().hasLabel(‘person’).has(‘EmployeeID’,2).inE(‘reportsto’).outV().hasLabel(‘person’).values(‘EmployeeID’)

g.V().hasLabel(‘person’).has(‘EmployeeID’,2).inE(‘reportsto’).outV().hasLabel(‘person’).inE(‘reportsto’).outV().hasLabel(‘person’).values(‘EmployeeID’).dedup()

--[We then get a union of these 2 query’s result together and get all employees that has Employee 2 as their direct and indirect manager]
