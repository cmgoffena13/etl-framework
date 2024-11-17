USE ETL_Metadata
GO

DROP PROCEDURE IF EXISTS etl.AuditAccuracyBatch_I
GO
CREATE PROCEDURE etl.AuditAccuracyBatch_I @ParentRunTimeID BIGINT,
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
			@AuditType VARCHAR(150) = 'Accuracy',
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

	/* Grab necessary accuracy rule information */
	DROP TABLE IF EXISTS #AccuracyRules
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
	a.MinimumBound,
	a.MaximumBound,
	ISNULL(JSON_VALUE(p.PipelineArgs, '$.info.audit.batch_size'), @DefaultBatchSize)  AS _BatchSize, /*Pipeline Args*/
	0 AS Complete /* For cursor work */
	INTO #AccuracyRules
	FROM etl.AuditColumnRule AS a
	INNER JOIN CTE
		ON CTE.AuditColumnRuleID = a.AuditColumnRuleID
	INNER JOIN etl.Pipeline AS p
		ON p.PipelineID = a.PipelineID
	INNER JOIN etl.Address AS ap
		ON ap.AddressID = p.TargetAddressID

	/* Begin string construction for dynamic query */
	DECLARE @TargetTable NVARCHAR(150) = (SELECT DISTINCT CONCAT_WS('.', DatabaseName, SchemaName, TableName) FROM #AccuracyRules),
			@PrimaryKey NVARCHAR(50) = (SELECT DISTINCT PrimaryKey FROM #AccuracyRules), 
			@AggColumnList NVARCHAR(MAX),
			@CountColumnList NVARCHAR(MAX),
			@Agg_List NVARCHAR(MAX) = '',
			@Count_List NVARCHAR(MAX),
			@BatchSize INT = (SELECT DISTINCT _BatchSize FROM #AccuracyRules),
			@RecordCount INT

	/* Each list needs unique tag added to prevent identity clash */
	SET @AggColumnList = (SELECT STRING_AGG(AuditColumn + '#Violation', ', ') WITHIN GROUP (ORDER BY AuditColumnRuleID) FROM #AccuracyRules)
	SET @CountColumnList = (SELECT STRING_AGG(AuditColumn + '#Count', ', ') WITHIN GROUP (ORDER BY AuditColumnRuleID) FROM #AccuracyRules)	
	
	SET @Count_List = (SELECT STRING_AGG('COUNT(' + AuditColumn + ') AS ' + AuditColumn + '#Count', ', ') WITHIN GROUP (ORDER BY AuditColumnRuleID) FROM #AccuracyRules)

	DECLARE @ColumnName VARCHAR(50), @MinimumBound VARCHAR(50), @MaximumBound VARCHAR(50), @RowNum INT
	DECLARE accuracy_cursor CURSOR FOR
		SELECT AuditColumn, MinimumBound, MaximumBound
		FROM #AccuracyRules
		WHERE Complete=0
		ORDER BY AuditColumnRuleID

	OPEN accuracy_cursor
	FETCH NEXT FROM accuracy_cursor INTO
	@ColumnName, @MinimumBound, @MaximumBound

	/* Form case strings for bound checks */
	WHILE @@FETCH_STATUS = 0
	BEGIN
		IF @MaximumBound IS NULL
		BEGIN /* Test if we need to wrap with quotes */
			IF TRY_CAST(@MinimumBound AS DATETIMEOFFSET(2)) IS NOT NULL
				OR TRY_CAST(@MinimumBound AS TIME(7)) IS NOT NULL
				BEGIN
					SET @Agg_List += 'SUM(CASE WHEN ' + @ColumnName + ' <= ' + '''' + @MinimumBound + '''' + ' THEN 1 ELSE 0 END) AS ' + @ColumnName + '#Violation,' + CHAR(13)
				END
				ELSE
				BEGIN
					SET @Agg_List += 'SUM(CASE WHEN ' + @ColumnName + ' <= ' + @MinimumBound + ' THEN 1 ELSE 0 END) AS ' + @ColumnName + '#Violation,' + CHAR(13)
				END
		END
		IF @MinimumBound IS NULL
		BEGIN
			IF TRY_CAST(@MaximumBound AS DATETIMEOFFSET(2)) IS NOT NULL
				OR TRY_CAST(@MaximumBound AS TIME(7)) IS NOT NULL
				BEGIN
					SET @Agg_List += 'SUM(CASE WHEN ' + @ColumnName + ' >= ' + '''' + @MaximumBound + '''' + ' THEN 1 ELSE 0 END) AS ' + @ColumnName + '#Violation,' + CHAR(13)
				END
				ELSE
				BEGIN
					SET @Agg_List += 'SUM(CASE WHEN ' + @ColumnName + ' >= ' + @MaximumBound + ' THEN 1 ELSE 0 END) AS ' + @ColumnName + '#Violation,' + CHAR(13)
				END
		END
		IF @MinimumBound IS NOT NULL AND @MaximumBound IS NOT NULL
		BEGIN
			IF (TRY_CAST(@MaximumBound AS DATETIMEOFFSET(2)) IS NOT NULL
				OR TRY_CAST(@MaximumBound AS TIME(7)) IS NOT NULL)
				AND 
					(TRY_CAST(@MinimumBound AS DATETIMEOFFSET(2)) IS NOT NULL
					OR TRY_CAST(@MinimumBound AS TIME(7)) IS NOT NULL)
				BEGIN /* If boths are time stamps, wrap around quotes */
					SET @Agg_List += 'SUM(CASE WHEN ' + @ColumnName + ' >= ' + '''' + @MaximumBound + '''' + ' OR ' + @ColumnName + ' <= ' + '''' + @MinimumBound + '''' + ' THEN 1 ELSE 0 END) AS ' + @ColumnName + '#Violation,' + CHAR(13)
				END
			ELSE
				BEGIN /* otherwise don't add quotes */
					SET @Agg_List += 'SUM(CASE WHEN ' + @ColumnName + ' >= ' + @MaximumBound + ' OR ' + @ColumnName + ' <= ' + @MinimumBound + ' THEN 1 ELSE 0 END) AS ' + @ColumnName + '#Violation,' + CHAR(13)
				END
		END

		IF @Debug = 1
			PRINT @Agg_List

		UPDATE #AccuracyRules
		SET Complete=1
		WHERE AuditColumn = @ColumnName

		FETCH NEXT FROM accuracy_cursor INTO
		@ColumnName, @MinimumBound, @MaximumBound

	END

	CLOSE accuracy_cursor
	DEALLOCATE accuracy_cursor

	/* Form Dynamic Query Loop */
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
		ColumnName VARCHAR(50),
		RecordCount BIGINT,
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
		
			/* Double unpivot and join back query string */
			SET @SQLQuery = '
			DROP TABLE IF EXISTS #temp
			;WITH CTE AS (
			SELECT
			PKID
			FROM #Incremental
			WHERE RowNumber >= @MinWindow
				AND RowNumber < @MaxWindow
			)
			SELECT
			{agg_list}
			{count_list}
			INTO #temp
			FROM ' + @TargetTable + '
			INNER JOIN CTE
				ON CTE.PKID = ' + @TargetTable + '.' + @PrimaryKey + '
		
			DROP TABLE IF EXISTS #first
			SELECT
			REPLACE(ColumnName, ''#Count'', '''') AS ColumnName,
			RecordCount
			INTO #first
			FROM (
			SELECT
			{count_column_list}
			FROM #temp 
			) AS Subquery
			UNPIVOT
			( RecordCount FOR ColumnName IN (
			{count_column_list}
			)) AS pvt
		
			DROP TABLE IF EXISTS #second
			SELECT
			REPLACE(ColumnName, ''#Violation'', '''') AS ColumnName,
			RecordViolationCount
			INTO #second
			FROM (
			SELECT
			{agg_column_list}
			FROM #temp 
			) AS Subquery
			UNPIVOT
			( RecordViolationCount FOR ColumnName IN (
			{agg_column_list}
			)) AS pvt

			DROP TABLE #temp

			INSERT INTO #Results_initial (ColumnName, RecordCount, RecordViolationCount)
			SELECT
			f.ColumnName,
			f.RecordCount,
			s.RecordViolationCount
			FROM #first AS f
			INNER JOIN #second AS s
				ON s.ColumnName = f.ColumnName'

			/* inject constructed column lists and agg lists into string */
			SET @SQLQuery = REPLACE(@SQLQuery, '{agg_list}', @Agg_List)
			SET @SQLQuery = REPLACE(@SQLQuery, '{agg_column_list}', @AggColumnList)
			SET @SQLQuery = REPLACE(@SQLQuery, '{count_list}', @Count_List)
			SET @SQLQuery = REPLACE(@SQLQuery, '{count_column_list}', @CountColumnList)
		
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
		
			/* Double unpivot and join back query string */
			SET @SQLQuery = '
			DROP TABLE IF EXISTS #temp
			SELECT
			{agg_list}
			{count_list}
			INTO #temp
			FROM ' + @TargetTable + '
			WHERE ' + @PrimaryKey + ' >= @MinWindow
			AND ' + @PrimaryKey + ' < @MaxWindow;
		
			DROP TABLE IF EXISTS #first
			SELECT
			REPLACE(ColumnName, ''#Count'', '''') AS ColumnName,
			RecordCount
			INTO #first
			FROM (
			SELECT
			{count_column_list}
			FROM #temp 
			) AS Subquery
			UNPIVOT
			( RecordCount FOR ColumnName IN (
			{count_column_list}
			)) AS pvt
		
			DROP TABLE IF EXISTS #second
			SELECT
			REPLACE(ColumnName, ''#Violation'', '''') AS ColumnName,
			RecordViolationCount
			INTO #second
			FROM (
			SELECT
			{agg_column_list}
			FROM #temp 
			) AS Subquery
			UNPIVOT
			( RecordViolationCount FOR ColumnName IN (
			{agg_column_list}
			)) AS pvt

			DROP TABLE #temp

			INSERT INTO #Results_initial (ColumnName, RecordCount, RecordViolationCount)
			SELECT
			f.ColumnName,
			f.RecordCount,
			s.RecordViolationCount
			FROM #first AS f
			INNER JOIN #second AS s
				ON s.ColumnName = f.ColumnName'

			/* inject constructed column lists and agg lists into string */
			SET @SQLQuery = REPLACE(@SQLQuery, '{agg_list}', @Agg_List)
			SET @SQLQuery = REPLACE(@SQLQuery, '{agg_column_list}', @AggColumnList)
			SET @SQLQuery = REPLACE(@SQLQuery, '{count_list}', @Count_List)
			SET @SQLQuery = REPLACE(@SQLQuery, '{count_column_list}', @CountColumnList)
		
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
	SET TRANSACTION ISOLATION LEVEL READ COMMITTED

	CREATE NONCLUSTERED INDEX IX_#Results_initial 
		ON #Results_initial (ColumnName) INCLUDE (RecordCount, RecordViolationCount)

	/* Aggregate the results */
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
		@RunTimeID,
		cr.AuditColumnRuleID,
		@DateRecorded,
		@HourRecorded,
		r.RecordCount,
		r.RecordViolationCount,
		CAST(CAST((r.RecordCount - r.RecordViolationCount) AS DECIMAL) / CAST(r.RecordCount AS DECIMAL) AS DECIMAL(20,4)) AS ResultScore
		FROM #Results AS r
		INNER JOIN #AccuracyRules AS cr
			ON cr.AuditColumn = r.ColumnName

	END
	ELSE
	BEGIN
		/* Used to update LastAuditColumnLogID */
		DECLARE @AccuracyTable TABLE (
			AuditColumnRuleID INT,
			AuditColumnLogID BIGINT
		)

		INSERT INTO etl.AuditColumnLog(
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
		INTO @AccuracyTable
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
		INNER JOIN #AccuracyRules AS cr
			ON cr.AuditColumn = r.ColumnName

		DROP TABLE #AccuracyRules
		DROP TABLE #Results

		UPDATE A
		SET LastAuditColumnLogID = t.AuditColumnLogID
		FROM etl.AuditColumnRule AS A
		INNER JOIN @AccuracyTable AS t
			ON t.AuditColumnRuleID = A.AuditColumnRuleID

	END

END