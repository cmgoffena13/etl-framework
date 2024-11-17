USE ETL_Metadata
GO

--TRUNCATE TABLE test.SourceTable
/* Create Fake new data */
DECLARE @PriceMaxConstraint INT = 2000,
		@MinRecords INT = 1000000,
		@MaxRecords INT = 1000000,
		@Records INT

SET @Records = (ROUND(((@MaxRecords - @MinRecords - 1) * RAND() + @MinRecords), 0))

;WITH CTE AS (
	SELECT
	1 AS IDCount,
	RIGHT(NEWID(), 100) AS IDName,
	RAND(CHECKSUM(NEWID())) * @PriceMaxConstraint AS Price
	UNION ALL
	SELECT
	IDCount +1 AS IDCount,
	RIGHT(NEWID(), 100) AS IDName,
	RAND(CHECKSUM(NEWID())) * @PriceMaxConstraint AS Price
	FROM CTE
	WHERE IDCount < @Records
	)
INSERT INTO test.SourceTable (EventValue, EventReason, CreatedDate)
SELECT
CAST(Price AS DECIMAL(17,2)),
CAST(IDName AS VARCHAR(50)),
SYSDATETIMEOFFSET()
FROM CTE
OPTION (MAXRECURSION 0)

;WITH CTE AS (
	SELECT TOP (10000)
	EventID,
	EventValue
	FROM test.SourceTable
	ORDER BY EventID ASC
)
UPDATE CTE
SET EventValue = EventValue + 1

DECLARE @IgnoreMessage VARBINARY(128)
SET @IgnoreMessage = CAST('Ignore' AS VARBINARY(128))

/* deleting records, and marking them as changes to ignore */
;WITH CHANGE_TRACKING_CONTEXT (@IgnoreMessage),
CTE AS (
	SELECT TOP 10
	EventID
	FROM test.SourceTable
)
DELETE CTE

;WITH CTE AS (
	SELECT TOP 100
	EventID
	FROM test.SourceTable
)
DELETE CTE


;WITH CTE AS (
	SELECT
	1 AS IDCount,
	RIGHT(NEWID(), 100) AS IDName,
	RAND(CHECKSUM(NEWID())) * @PriceMaxConstraint AS Price
	UNION ALL
	SELECT
	IDCount +1 AS IDCount,
	RIGHT(NEWID(), 100) AS IDName,
	RAND(CHECKSUM(NEWID())) * @PriceMaxConstraint AS Price
	FROM CTE
	WHERE IDCount < @Records
	)
INSERT INTO test.SourceTable2 (Revenue, LastSaleEventReason, CreatedDate)
SELECT
CAST(Price AS DECIMAL(17,2)),
CAST(IDName AS VARCHAR(50)),
SYSDATETIMEOFFSET()
FROM CTE
OPTION (MAXRECURSION 0)

;WITH CTE AS (
	SELECT TOP (10000)
	SaleID,
	Revenue
	FROM test.SourceTable2
	ORDER BY SaleID ASC
)
UPDATE CTE
SET Revenue = Revenue + 1