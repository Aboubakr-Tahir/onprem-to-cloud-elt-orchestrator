USE [master];
GO

RESTORE DATABASE [AdventureWorksLT2025]
FROM DISK = N'/var/opt/mssql/data/AdventureWorksLT2019.bak'
WITH FILE = 1,
MOVE N'AdventureWorksLT2019_Data' TO N'/var/opt/mssql/data/AdventureWorksLT2025.mdf',
MOVE N'AdventureWorksLT2019_Log' TO N'/var/opt/mssql/data/AdventureWorksLT2025_log.ldf',
NOUNLOAD, STATS = 5;
GO


RESTORE FILELISTONLY FROM DISK = N'/var/opt/mssql/data/AdventureWorksLT2019.bak';

use AdventureWorksLT2025;
select * from SalesLT.Customer;

SELECT TABLE_SCHEMA, TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE = 'BASE TABLE';