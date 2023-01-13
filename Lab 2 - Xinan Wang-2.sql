-- DAMG 7275 Lab 2  Xinan Wang 
-- Part 1
 /*
   Given two sets of tables as defined below, build a data pipeline 
   using the SQL MERGE function and data refresh jobs to synchronize 
   the data stored in the destination with the data stored in the source
   on a schedule. Keep an audit trail of what's changed in the destination
   set of tables using the SQL OUTPUT command. The two data sets exist
   in two different databases.
   Create a stored procedure containing the SQL code. The data pipeline 
   will be based on the stored procedure.
   Regarding the UPDATE command, in the destination data set,
   only CounselDate and ModifiedDate may change. INSERT and DELETE
   will affect all columns of the destination data set.
   Think about:
   1) How to prepare the data format at the source for data synch
   2) Where to put the audit tables?
   3) In which database to create the stored procedure?
 */
 /*
 Steps:
 1) Create a source database and a destination database
 2) Create a stored procedure in your database containing MERGE and OUTPUT
 3) Use Azure Data Studio to establish the data pipeline
 4) Set up a SQL job in Azure Data Studio to synch data
 5) Set up a Jupyter Notebook job in Azure Data Studio to synch data
 */
 /*
 Submit:
 1) SQL code
 2) Screenshots of the two configured data synch jobs
 */
/*
Helpful Videos for this lab:
Install and Use Azure Data Studio
https://www.youtube.com/watch?
v=YRK0hOssrfI&list=PL3gqFGmaw_0HgOyn_BuC3qZ87dalVF2fK&index=7
Jupyter Notebook and SQL
https://www.youtube.com/watch?v=6WO-zo6XR0k&list=PL-zncNSJGgbjuUF-
OyzCR6eKs4o_Fq7zj&index=27
How to Install SQL Server Agent Extension for Azure Data Studio_default
https://www.youtube.com/watch?v=cBFdoe2Zk6w&list=PL-zncNSJGgbjuUF-
OyzCR6eKs4o_Fq7zj&index=19
How to Create SQL Job in Azure Data Studio_default
https://www.youtube.com/watch?v=gYzG3XWskSY&list=PL-zncNSJGgbjuUF-
OyzCR6eKs4o_Fq7zj&index=23
How to Create Jupyter Notebook Job in Azure Data Studio_default
https://www.youtube.com/watch?v=msUKon2FTtY&list=PL-zncNSJGgbjuUF-
OyzCR6eKs4o_Fq7zj&index=22
*/

CREATE DATABASE XWSourceDB;
CREATE DATABASE XWDestinationDB;

USE XWSourceDB;

/* Source */
CREATE TABLE Client
(ClientID INT IDENTITY PRIMARY KEY,
 LastName VARCHAR(50),
 FirstName VARCHAR(50),
 Phone varchar(20));

CREATE TABLE Counseling
(CounselID INT IDENTITY PRIMARY KEY,
 ClientID INT REFERENCES Client(ClientID),
 CounselDate DATE,
 ModifiedDate DATETIME DEFAULT getdate())

USE XWSourceDB;

CREATE View vCounseling
WITH SCHEMABINDING
AS SELECT co.CounselID, cl.LastName, cl.FirstName, cl.Phone, 
  co.CounselDate, co.ModifiedDate 
FROM dbo.Client cl
JOIN dbo.Counseling co 
ON cl.ClientID = co.ClientID;

CREATE UNIQUE CLUSTERED INDEX vCon
 ON vCounseling (CounselID, LastName);

USE XWDestinationDB;

/* Destination */
CREATE TABLE CounselingReport
(CounselID int PRIMARY KEY,
 LastName VARCHAR(50),
 FirstName VARCHAR(50),
 Phone VARCHAR(20),
 CounselDate DATE,
 ModifiedDate DATETIME);

/* Audit Table */
CREATE TABLE DateAudit
(LogID INT IDENTITY,
 Action VARCHAR(10),
 CounselID INT,
 OldDate DATE,
 NewDate DATE,
 ChangedBy VARCHAR(50) DEFAULT original_login(),
 ChangeTime DATETIME DEFAULT GETDATE());

/* MERGE Source Table with Destination Table */
CREATE PROCEDURE syncData
AS
BEGIN 
    MERGE XWDestinationDB.dbo.CounselingReport cr
    USING (SELECT CounselID, CounselDate, ModifiedDate, LastName, FirstName, Phone
        FROM XWSourceDB.dbo.Client cl
        JOIN XWSourceDB.dbo.Counseling co
        ON cl.ClientID = co.ClientID) temp
    ON cr.CounselID = temp.CounselID
    WHEN MATCHED THEN UPDATE SET LastName = temp.LastName,
                                FirstName = temp.FirstName,
                                Phone = temp.Phone,
                                CounselDate = temp.CounselDate,
                                ModifiedDate = temp.ModifiedDate
    WHEN NOT MATCHED BY SOURCE THEN DELETE
    WHEN NOT MATCHED THEN 
        INSERT(CounselID, LastName, FirstName, Phone, CounselDate, ModifiedDate)
        VALUES(temp.CounselID, temp.LastName, temp.FirstName, temp.Phone, temp.CounselDate, temp.ModifiedDate)
    OUTPUT
        $action, ISNULL(Deleted.CounselID, Inserted.CounselID), Deleted.CounselDate,
        Inserted.CounselDate
    INTO XWDestinationDB.dbo.DateAudit(
        [Action],CounselID, OldDate, NewDate
    );
END;


-- Part 2
/*
   Given two sets of tables as defined below, build a data pipeline 
   using the SQL triggers to synchronize the data stored in the destination 
   with the data stored in the source in an event-driven and real-time manner. 
   The two data sets exist in two different databases. Reuse the two databases
   created in Part 1.
   Keep an audit trail of what's changed in the destination set of tables. 
   Think about:
   1) Where to put the audit tables?
   2) How many trigger(s) need to be created?
   3) In which database to create the trigger(s)?
    
   Submit the SQL code.
*/

USE XWSourceDB;

/* Source */
create table SaleOrder
(OrderID int identity primary key,
 OrderDate date,
 CustomerID int,
 Modified datetime);
create table OrderItem
(OrderID int references SaleOrder(OrderID),
 ItemID int,
 Quantity int,
 UnitPrice money
 primary key (OrderID, ItemID));

 USE XWDestinationDB;

/* Destination */
create table SaleOrderReport
(OrderID int primary key,
 OrderDate date,
 CustomerID int,
 Modified datetime);
create table OrderItemReport
(OrderID int,
 ItemID int,
 Quantity int,
 UnitPrice money
 primary key (OrderID, ItemID));

-- Audit Tables
CREATE TABLE AuditSaleOrder
 (
  Audit_PK  INT  IDENTITY(1,1) NOT NULL
  ,OrderID  INT  NOT NULL
  ,OldOrderDate date
  ,NewOrderDate date
  ,OldCustomerID int
  ,NewCustomerID int
  ,[Action] CHAR(6) NULL
  ,ActionTime DATETIME DEFAULT GETDATE()
 );
CREATE TABLE AuditOrderItem
 (
  Audit_PK  INT  IDENTITY(1,1) NOT NULL
  ,OrderID  INT  NOT NULL
  ,OldItemID int
  ,NewItemID int
  ,OldQuantity int
  ,NewQuantity int
  ,OldUnitPrice money
  ,NewUnitPrice money
  ,[Action] CHAR(6) NULL
  ,ActionTime DATETIME DEFAULT GETDATE()
 );


CREATE TRIGGER SalesOrderAuditTrigger 
ON  XWSourceDB.dbo.SaleOrder
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
	DECLARE @OrderId INT, @NewOrderDate Date, @NewCustomerID INT, @NewModified DATETIME, 
			@OldOrderDate Date, @OldCustomerID INT , @OldModified DATETIME

	SELECT @OrderId = OrderId, 
		   @NewOrderDate = OrderDate, 
		   @NewCustomerID = CustomerID,
		   @NewModified = Modified 
  	FROM inserted

	SELECT @OrderId = OrderId, 
		   @OldOrderDate = OrderDate, 
		   @OldCustomerID = CustomerID,
		   @OldModified = Modified 
    FROM deleted

	IF EXISTS(SELECT * FROM inserted)
		BEGIN
		IF EXISTS(SELECT * FROM deleted)
			-- start the update process
			BEGIN
				-- insert into sale order audit table
        		INSERT INTO XWDestinationDB.dbo.AuditSaleOrder(OrderID,OldOrderDate,NewOrderDate,OldCustomerID,NewCustomerID,[Action])
        		VALUES(@OrderID, @OldOrderDate, @NewOrderDate, @OldCustomerID, @NewCustomerID, 'update')
			    -- update the sale order report table
        		UPDATE XWDestinationDB.dbo.SaleOrderReport
			    SET OrderID = @OrderID, OrderDate = @NewOrderDate,CustomerID = @NewCustomerID, Modified = @NewModified
			    WHERE OrderID  = @OrderID
			END
		ELSE
			-- start the insert process
			BEGIN
				-- insert into sale order audit table
        		INSERT INTO XWDestinationDB.dbo.AuditSaleOrder(OrderID,OldOrderDate,NewOrderDate,OldCustomerID,NewCustomerID,[Action])
        		VALUES( @OrderID, @OldOrderDate, @NewOrderDate, @OldCustomerID, @NewCustomerID, 'insert')
			  	-- insert into the sale order report table
        		INSERT INTO XWDestinationDB.dbo.SaleOrderReport(OrderID, OrderDate,CustomerID, Modified)
			  	VALUES (@OrderID,@NewOrderDate,@NewCustomerID,@NewModified)
			 END
	END
	ELSE
		-- start the delete process
		BEGIN
			IF EXISTS(SELECT * FROM deleted)
			    -- insert into sale order audit table
        		INSERT INTO XWDestinationDB.dbo.AuditSaleOrder(OrderID,OldOrderDate,NewOrderDate,OldCustomerID,NewCustomerID,[Action])
        		VALUES( @OrderID, @OldOrderDate, @NewOrderDate, @OldCustomerID, @NewCustomerID,'delete')
        		-- delete from sale order report table
        		DELETE FROM XWDestinationDB.dbo.SaleOrderReport WHERE OrderID = @OrderID
		END
END; 



CREATE TRIGGER OrderItemAuditTrigger 
ON XWSourceDB.dbo.OrderItem
AFTER INSERT, UPDATE, DELETE
AS 
BEGIN
	DECLARE	@OrderID INT, @OldItemID INT, @NewItemID INT, @OldQuantity INT, @NewQuantity INT, @OldUnitPrice money, @NewUnitPrice money

	SELECT @OrderID = OrderID, 
		   @NewItemID = ItemID, 
		   @NewQuantity = Quantity, 
		   @NewUnitPrice = UnitPrice
  	FROM inserted

	SELECT @OrderID = OrderID, 
	       @OldItemID = ItemID, 
	       @OldQuantity = Quantity, 
	       @OldUnitPrice = UnitPrice
    FROM deleted

	IF EXISTS(SELECT * FROM inserted)
		BEGIN
		IF EXISTS(SELECT * FROM deleted)
			-- start the update process
			BEGIN
				INSERT INTO XWDestinationDB.dbo.AuditOrderItem(OrderID,OldItemID,NewItemID,OldQuantity,NewQuantity,OldUnitPrice,NewUnitPrice,[Action])
				Values( @OrderID, @OldItemID,@NewItemID,@OldQuantity,@NewQuantity,@OldUnitPrice,@NewUnitPrice, 'update')
				
				UPDATE XWDestinationDB.dbo.OrderItemReport 
        		SET OrderID = @OrderID, ItemID = @NewItemID, Quantity = @NewQuantity, UnitPrice = @NewUnitPrice
				WHERE OrderID = @OrderID
			END
    	ELSE
    		-- start the insert process
			 BEGIN
				INSERT INTO XWDestinationDB.dbo.AuditOrderItem(OrderID,OldItemID,NewItemID,OldQuantity,NewQuantity,OldUnitPrice,NewUnitPrice,[Action])
				VALUES( @OrderID, @OldItemID,@NewItemID,@OldQuantity,@NewQuantity,@OldUnitPrice,@NewUnitPrice,  'insert')
				
				INSERT INTO XWDestinationDB.dbo.OrderItemReport (OrderID, ItemID,Quantity, UnitPrice)
				VALUES(@OrderID,@NewItemID,@NewQuantity,@NewUnitPrice)
			END
	END
	ELSE
		-- start the delete process
		BEGIN
		IF EXISTS(SELECT * FROM deleted)
			INSERT INTO XWDestinationDB.dbo.AuditOrderItem(OrderID,OldItemID,NewItemID,OldQuantity,NewQuantity,OldUnitPrice,NewUnitPrice,[Action])
            VALUES(@OrderID, @OldItemID,@NewItemID,@OldQuantity,@NewQuantity,@OldUnitPrice,@NewUnitPrice, 'delete')
			
            DELETE FROM XWDestinationDB.dbo.OrderItemReport WHERE OrderID = @OrderID
		END
END;


