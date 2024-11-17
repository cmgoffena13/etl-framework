USE ETL_Metadata
GO

DROP PROCEDURE IF EXISTS etl.AuditCompletenessBatch_I
GO
CREATE PROCEDURE etl.AuditCompletenessBatch_I @ParentRunTimeID BIGINT,
											@RunTimeID BIGINT,
											 @PipelineID INT,
											 @FullLoad BIT,
											 @Debug BIT = 0,
											 @DefaultBatchSize INT = 100000
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DECLARE @Timestamp DATETIMEOFFSET(2),
			@DateRecorded DATE,
			@HourRecorded TINYINT,
			@ChunkOut BIT = 1,
			@AuditType VARCHAR(150) = 'Completeness',
			@AuditTypeID TINYINT,
			@ErrorMessage VARCHAR(100)

	IF @FullLoad = 0 AND @ParentRunTimeID IS NULL
	BEGIN
		SET @ErrorMessage = '@ParentRunTimeID is required for incremental audits, @FullLoad=0';
		THROW 51000, @ErrorMessage, 1;
	END

	/* Can't depend on certain ID, so seek based upon text */
	SELECT @AuditTypeID = AuditTypeID
	FROM etl.AuditType
	WHERE AuditType = @AuditType

	/* Use Pipeline start of the audit Pipeline to determine date/hour comparisons */
	SELECT @Timestamp = PipelineStart
	FROM etl.RunTime
	WHERE RunTimeID = @RunTimeID

	SET @DateRecorded = CAST(@Timestamp AS DATE)
	SET	@HourRecorded = DATEPART(HOUR, @Timestamp)

	/* Grab necessary completeness rule information */
	DROP TABLE IF EXISTS #CompletenessRules
	;WITH CTE AS (
	SELECT
	AuditColumnRuleID
	FROM etl.AuditColumnRule
	WHERE PipelineID = @PipelineID
		AND AuditTypeID = @AuditTypeID
		AND Active = 1
	)
	SELECT
	a.AuditColumnRuleID,
	ap.DatabaseName,
	ap.SchemaName,
	ap.TableName,
	ap.PrimaryKey,
	a.AuditColumn,
	ISNULL(JSON_VALUE(p.PipelineArgs, '$.info.audit.batch_size'), @DefaultBatchSize)  AS _BatchSize /*Pipeline Args*/
	INTO #CompletenessRules
	FROM etl.AuditColumnRule AS a
	INNER JOIN CTE ON
		CTE.AuditColumnRuleID = a.AuditColumnRuleID
	INNER JOIN etl.Pipeline AS p
		ON p.PipelineID = a.PipelineID
	INNER JOIN etl.Address AS ap
		ON ap.AddressID = p.TargetAddressID

	/* Begin string construction for dynamic query */
	DECLARE @TargetTable NVARCHAR(150) = (SELECT DISTINCT CONCAT_WS('.', DatabaseName, SchemaName, TableName) FROM #CompletenessRules),
			@PrimaryKey NVARCHAR(50) = (SELECT DISTINCT PrimaryKey FROM #CompletenessRules), 
			@ColumnList NVARCHAR(MAX),
			@Agg_List NVARCHAR(MAX),
			@BatchSize INT = (SELECT DISTINCT _BatchSize FROM #CompletenessRules),
			@RecordCount INT

	SELECT
	@ColumnList = STRING_AGG(AuditColumn, ',') WITHIN GROUP (ORDER BY AuditColumnRuleID),
	@Agg_List = STRING_AGG( 'SUM(CASE WHEN ' + AuditColumn + ' IS NULL THEN 1 ELSE 0 END) AS ' + AuditColumn + '', ',') WITHIN GROUP (ORDER BY AuditColumnRuleID)
	FROM #CompletenessRules

	DECLARE @SQLQuery NVARCHAR(MAX),
			@MinWindow INT,
			@MaxWindow INT,
			@Max INT

	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
	IF @FullLoad = 0 /* If incremental, only check parent Pipeline run time's data */
	BEGIN
		SET @MinWindow = 1
		SET @SQLQuery = '
		SELECT @Max = COUNT(*) 
		FROM ' + @TargetTable + '
		WHERE RunTimeID = ' + CAST(@ParentRunTimeID AS VARCHAR(50)) + ''

		IF @Debug=1
		BEGIN 
			PRINT @SQLQuery 
			EXEC sys.sp_executesql @SQLQuery, N'@Max INT OUTPUT', @Max = @Max OUTPUT 
		END 
		ELSE
		BEGIN 
			EXEC sys.sp_executesql @SQLQuery, N'@Max INT OUTPUT', @Max = @Max OUTPUT 
		END
	END
	ELSE
	BEGIN
		SET @SQLQuery = 'SELECT TOP 1 @MinWindow = ' + @PrimaryKey + ' FROM ' + @TargetTable + ' ORDER BY ' + @PrimaryKey + ' ASC'
		IF @Debug=1
		BEGIN 
			PRINT @SQLQuery 
			EXEC sys.sp_executesql @SQLQuery, N'@MinWindow INT OUTPUT', @MinWindow = @MinWindow OUTPUT
		END
		ELSE
		BEGIN 
			EXEC sys.sp_executesql @SQLQuery, N'@MinWindow INT OUTPUT', @MinWindow = @MinWindow OUTPUT 
		END
	
		SET @SQLQuery = 'SELECT TOP 1 @Max = ' + @PrimaryKey + ' FROM ' + @TargetTable + ' ORDER BY ' + @PrimaryKey + ' DESC'
		IF @Debug=1
		BEGIN 
			PRINT @SQLQuery 
			EXEC sys.sp_executesql @SQLQuery, N'@Max INT OUTPUT', @Max = @Max OUTPUT 
		END
		ELSE
		BEGIN 
			EXEC sys.sp_executesql @SQLQuery, N'@Max INT OUTPUT', @Max = @Max OUTPUT 
		END
	END

	SET @MaxWindow = @MinWindow + @BatchSize

	DROP TABLE IF EXISTS #Results_initial
	CREATE TABLE #Results_initial (
		RecordCount BIGINT,
		ColumnName VARCHAR(50),
		RecordViolationCount BIGINT
	)
	IF @FullLoad=0 /* If incremental, only check parent Pipeline run time's data */
	BEGIN
		DROP TABLE IF EXISTS #Incremental
		CREATE TABLE #Incremental (PKID BIGINT, RowNumber INT)
		SET @SQLQuery = '
		INSERT INTO #Incremental (PKID, RowNumber)
		SELECT
		' + @PrimaryKey + ',
		ROW_NUMBER() OVER (ORDER BY ' + @PrimaryKey + ') AS RowNumber
		FROM ' + @TargetTable + '
		WHERE RunTimeID = ' + CAST(@ParentRunTimeID AS VARCHAR(50)) + ''

		IF @Debug=1
		BEGIN 
			PRINT @SQLQuery 
		END
		ELSE
		BEGIN 
			EXEC sys.sp_executesql @SQLQuery 
		END

		WHILE @MinWindow < @Max
		BEGIN
			SET @SQLQuery = '
			;WITH CTE AS (
			SELECT
			PKID
			FROM #Incremental
			WHERE RowNumber >= @MinWindow
				AND RowNumber < @MaxWindow
			)
			INSERT INTO #Results_initial (RecordCount, ColumnName, RecordViolationCount)
			SELECT
			RecordCount,
			ColumnName,
			RecordViolationCount
			FROM (
			SELECT
			COUNT(*) AS RecordCount,
			{agg_list}
			FROM ' + @TargetTable + '
			INNER JOIN CTE
				ON CTE.PKID = ' + @TargetTable + '.' + @PrimaryKey + '
			) AS SubQuery
			UNPIVOT
			( RecordViolationCount FOR ColumnName IN (
			{column_list}
			)) AS pvt'

			/* inject constructed column list and agg list into string */
			SET @SQLQuery = REPLACE(@SQLQuery, '{agg_list}', @Agg_List)
			SET @SQLQuery = REPLACE(@SQLQuery, '{column_list}', @ColumnList)
		
			IF @Debug=1
			BEGIN 
				PRINT @SQLQuery 
			END
			ELSE
			BEGIN
				EXEC sys.sp_executesql @SQLQuery, 
				N'@MinWindow INT, @MaxWindow INT',
				@MinWindow = @MinWindow, @MaxWindow = @MaxWindow
			END

			SET @MinWindow = @MaxWindow
			SET @MaxWindow += @BatchSize

		END
	END
	ELSE /* Chunk out entire table if full load */
	BEGIN
		WHILE @MinWindow < @Max
		BEGIN
		
			/* simple chunked unpivot */
			SET @SQLQuery = '
			INSERT INTO #Results_initial (RecordCount, ColumnName, RecordViolationCount)
			SELECT
			RecordCount,
			ColumnName,
			RecordViolationCount
			FROM (
			SELECT
			COUNT(*) AS RecordCount,
			{agg_list}
			FROM ' + @TargetTable + '
			WHERE ' + @PrimaryKey + ' >= @MinWindow
			AND ' + @PrimaryKey + ' < @MaxWindow
			) AS SubQuery
			UNPIVOT
			( RecordViolationCount FOR ColumnName IN (
			{column_list}
			)) AS pvt'

			/* inject constructed column list and agg list into string */
			SET @SQLQuery = REPLACE(@SQLQuery, '{agg_list}', @Agg_List)
			SET @SQLQuery = REPLACE(@SQLQuery, '{column_list}', @ColumnList)
		
			IF @Debug=1 BEGIN 
				PRINT @SQLQuery 
			END
			ELSE
			BEGIN
				EXEC sys.sp_executesql @SQLQuery, 
				N'@MinWindow INT, @MaxWindow INT',
				@MinWindow = @MinWindow, @MaxWindow = @MaxWindow
			END

			SET @MinWindow = @MaxWindow
			SET @MaxWindow += @BatchSize

		END
	END
	SET TRANSACTION ISOLATION LEVEL READ COMMITTED

	CREATE NONCLUSTERED INDEX IX_#Results_initial 
		ON #Results_initial (RecordCount, ColumnName) INCLUDE (RecordViolationCount)

	/* aggregate up results */
	DROP TABLE IF EXISTS #Results
	SELECT
	ColumnName,
	SUM(RecordCount) AS RecordCount,
	SUM(RecordViolationCount) AS RecordViolationCount
	INTO #Results
	FROM #Results_initial
	GROUP BY ColumnName

	DROP TABLE #Results_initial

	IF @Debug=1
	BEGIN

		SELECT
		@RunTimeID AS RunTimeID,
		cr.AuditColumnRuleID,
		@DateRecorded AS DateRecorded,
		@HourRecorded AS HourRecorded, 
		r.RecordCount,
		r.RecordViolationCount,
		CAST(CAST((r.RecordCount - r.RecordViolationCount) AS DECIMAL) / CAST(r.RecordCount AS DECIMAL) AS DECIMAL(20,4)) AS ResultScore
		FROM #Results AS r
		INNER JOIN #CompletenessRules AS cr
			ON cr.AuditColumn = r.ColumnName

	END
	ELSE
	BEGIN
		/* Used to update LastAuditColumnLogID */
		DECLARE @CompletenessTable TABLE (
			AuditColumnRuleID INT,
			AuditColumnLogID BIGINT
		)

		INSERT INTO etl.AuditColumnLog (
			RunTimeID,
			AuditColumnRuleID,
			DateRecorded,
			HourRecorded,
			RecordCount,
			RecordViolationCount,
			ResultScore,
			FullLoad,
			ChunkOut
		)
		OUTPUT inserted.AuditColumnRuleID, inserted.AuditColumnLogID
		INTO @CompletenessTable
		SELECT
		@RunTimeID,
		cr.AuditColumnRuleID,
		@DateRecorded,
		@HourRecorded,
		r.RecordCount,
		r.RecordViolationCount,
		CAST(CAST((r.RecordCount - r.RecordViolationCount) AS DECIMAL) / CAST(r.RecordCount AS DECIMAL) AS DECIMAL(20,4)) AS ResultScore,
		@FullLoad,
		@ChunkOut
		FROM #Results AS r
		INNER JOIN #CompletenessRules AS cr
			ON cr.AuditColumn = r.ColumnName

		DROP TABLE #CompletenessRules
		DROP TABLE #Results

		UPDATE A
		SET LastAuditColumnLogID = t.AuditColumnLogID
		FROM etl.AuditColumnRule AS A
		INNER JOIN @CompletenessTable AS t
			ON t.AuditColumnRuleID = A.AuditColumnRuleID

	END

END