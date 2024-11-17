THROW 51000, 'Make sure to designate the right database and then comment this out', 1;

/* Took 20 seconds on my computer; Creates ~1 million records */
DECLARE @PriceMaxConstraint INT = 2000,
		@MinRecords INT = 1000000,
		@MaxRecords INT = 1500000,
		@Records INT

SET @Records = (ROUND(((@MaxRecords - @MinRecords - 1) * RAND() + @MinRecords), 0))

/* Create new records */
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

/* Update records */
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

/* deleting records and not marking them */
;WITH CTE AS (
	SELECT TOP 100
	EventID
	FROM test.SourceTable
)
DELETE CTE