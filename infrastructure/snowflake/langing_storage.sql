CREATE DATABASE IF NOT EXISTS ADVENTUREWORKS_DW;

USE DATABASE ADVENTUREWORKS_DW;
CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;

CREATE OR REPLACE STORAGE INTEGRATION azure_adls_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = 'YOUR_ID'
  STORAGE_ALLOWED_LOCATIONS = ('azure://YOUR_STORAGE_LOCATION');

DESC STORAGE INTEGRATION azure_adls_integration;

USE SCHEMA BRONZE;

CREATE OR REPLACE STAGE my_azure_stage
  STORAGE_INTEGRATION = azure_adls_integration
  URL = 'azure://YOUR_STORAGE_LOCATION';

LS @my_azure_stage;

USE DATABASE ADVENTUREWORKS_DW;
USE SCHEMA SILVER;

CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE TABLE SILVER.Customer (
    CustomerID INT,
    NameStyle BOOLEAN,
    Title VARCHAR(10),
    FirstName VARCHAR(50),
    MiddleName VARCHAR(50),
    LastName VARCHAR(50),
    Suffix VARCHAR(10),
    CompanyName VARCHAR(128),
    SalesPerson VARCHAR(256),
    EmailAddress VARCHAR(50),
    Phone VARCHAR(25),
    PasswordHash VARCHAR(128),
    PasswordSalt VARCHAR(10),
    rowguid VARCHAR(64),
    ModifiedDate TIMESTAMP_NTZ
);  

COPY INTO SILVER.Customer
FROM @BRONZE.my_azure_stage/adventureworks/Customer/Customer.csv
FILE_FORMAT = (FORMAT_NAME = my_csv_format)
ON_ERROR = 'CONTINUE'; 

CREATE OR REPLACE TABLE SILVER.Address (
    AddressID INT, AddressLine1 VARCHAR(60), AddressLine2 VARCHAR(60), City VARCHAR(30),
    StateProvince VARCHAR(50), CountryRegion VARCHAR(50), PostalCode VARCHAR(15),
    rowguid VARCHAR(64), ModifiedDate TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE SILVER.CustomerAddress (
    CustomerID INT, AddressID INT, AddressType VARCHAR(50),
    rowguid VARCHAR(64), ModifiedDate TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE SILVER.Product (
    ProductID INT, Name VARCHAR(50), ProductNumber VARCHAR(25), Color VARCHAR(15),
    StandardCost NUMBER(19,4), ListPrice NUMBER(19,4), Size VARCHAR(5), Weight NUMBER(18,2),
    ProductCategoryID INT, ProductModelID INT, SellStartDate TIMESTAMP_NTZ,
    SellEndDate TIMESTAMP_NTZ, DiscontinuedDate TIMESTAMP_NTZ, 
    ThumbNailPhoto VARCHAR, 
    ThumbnailPhotoFileName VARCHAR(50), rowguid VARCHAR(64), ModifiedDate TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE SILVER.ProductCategory (
    ProductCategoryID INT, ParentProductCategoryID INT, Name VARCHAR(50),
    rowguid VARCHAR(64), ModifiedDate TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE SILVER.ProductDescription (
    ProductDescriptionID INT, Description VARCHAR(400),
    rowguid VARCHAR(64), ModifiedDate TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE SILVER.ProductModel (
    ProductModelID INT, Name VARCHAR(50), CatalogDescription VARIANT, -- XML mappé en VARIANT
    rowguid VARCHAR(64), ModifiedDate TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE SILVER.ProductModelProductDescription (
    ProductModelID INT, ProductDescriptionID INT, Culture VARCHAR(6),
    rowguid VARCHAR(64), ModifiedDate TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE SILVER.SalesOrderDetail (
    SalesOrderID INT, SalesOrderDetailID INT, OrderQty INT, ProductID INT,
    UnitPrice NUMBER(38,4), UnitPriceDiscount NUMBER(38,4), LineTotal NUMBER(38,2),
    rowguid VARCHAR(64), ModifiedDate TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE SILVER.SalesOrderHeader (
    SalesOrderID INT, RevisionNumber INT, OrderDate TIMESTAMP_NTZ, DueDate TIMESTAMP_NTZ,
    ShipDate TIMESTAMP_NTZ, Status INT, OnlineOrderFlag BOOLEAN, SalesOrderNumber VARCHAR(25),
    PurchaseOrderNumber VARCHAR(25), AccountNumber VARCHAR(15), CustomerID INT,
    ShipToAddressID INT, BillToAddressID INT, ShipMethod VARCHAR(50),
    CreditCardApprovalCode VARCHAR(15), SubTotal NUMBER(19,4), TaxAmt NUMBER(19,4),
    Freight NUMBER(19,4), TotalDue NUMBER(19,4), Comment VARCHAR,
    rowguid VARCHAR(64), ModifiedDate TIMESTAMP_NTZ
);

USE DATABASE ADVENTUREWORKS_DW;
USE SCHEMA GOLD;

CREATE OR REPLACE VIEW GOLD.FACT_SALES AS
SELECT 
    H.SalesOrderID,
    H.OrderDate,
    D.ProductID,
    H.CustomerID,
    A.City,
    A.CountryRegion,
    D.OrderQty,
    D.UnitPrice,
    D.LineTotal,
    (D.UnitPrice * D.OrderQty) AS GrossAmount
FROM SILVER.SalesOrderHeader H
JOIN SILVER.SalesOrderDetail D ON H.SalesOrderID = D.SalesOrderID
JOIN SILVER.Address A ON H.ShipToAddressID = A.AddressID;

select * from ADVENTUREWORKS_DW.GOLD.FACT_SALES;

CREATE OR REPLACE VIEW GOLD.DIM_PRODUCT AS
SELECT 
    P.ProductID,
    P.Name AS ProductName,
    P.ProductNumber,
    C.Name AS CategoryName,
    P.Color,
    P.ListPrice
FROM SILVER.Product P
LEFT JOIN SILVER.ProductCategory C ON P.ProductCategoryID = C.ProductCategoryID;

CREATE OR REPLACE VIEW GOLD.DIM_CUSTOMER AS
SELECT 
    CustomerID,
    CompanyName,
    FirstName || ' ' || LastName AS FullName,
    SalesPerson
FROM SILVER.Customer;

DROP SCHEMA IF EXISTS ADVENTUREWORKS_DW.SILVER_STAGING;
DROP SCHEMA IF EXISTS ADVENTUREWORKS_DW.SILVER_GOLD;
DROP SCHEMA IF EXISTS ADVENTUREWORKS_DW.GOLD; ADVENTUREWORKS_DW.SILVER_GOLDADVENTUREWORKS_DW.SILVER