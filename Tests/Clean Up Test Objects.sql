USE ETL_Metadata
GO

DECLARE @Schema VARCHAR(100) = 'test',
		@Debug BIT = 1

DECLARE @TableName VARCHAR(100)
DECLARE test_cursor CURSOR FOR
	SELECT
	@Schema + '.' + t.name
	FROM sys.tables AS t
	INNER JOIN sys.schemas AS s
		ON s.schema_id = t.schema_id
	WHERE s.name = @Schema

OPEN test_cursor
FETCH NEXT FROM test_cursor INTO @TableName

DECLARE @SQL NVARCHAR(MAX)
WHILE @@FETCH_STATUS = 0
BEGIN

	SET @SQL = 'DROP TABLE IF EXISTS ' + @TableName + ';'
	IF @Debug = 1
	BEGIN
		PRINT @SQL
	END
	ELSE
	BEGIN
		EXEC sys.sp_executesql @SQL
	END
	
	FETCH NEXT FROM test_cursor INTO @TableName
END

CLOSE test_cursor
DEALLOCATE test_cursor


DECLARE @StoredProcedure VARCHAR(100)

DECLARE sproc_cursor CURSOR FOR
SELECT
@Schema + '.' + p.name
FROM sys.procedures AS p
INNER JOIN sys.schemas AS s
	ON s.schema_id = p.schema_id
WHERE s.name = @Schema

OPEN sproc_cursor
FETCH NEXT FROM sproc_cursor INTO @StoredProcedure

WHILE @@FETCH_STATUS = 0
BEGIN
	SET @SQL = 'DROP PROCEDURE IF EXISTS ' + @StoredProcedure
	IF @Debug = 1
	BEGIN
		PRINT @SQL
	END
	ELSE
	BEGIN
		EXEC sys.sp_executesql @SQL
	END

	FETCH NEXT FROM sproc_cursor INTO @StoredProcedure
END

CLOSE sproc_cursor
DEALLOCATE sproc_cursor