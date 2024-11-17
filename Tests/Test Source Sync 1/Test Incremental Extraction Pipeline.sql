USE ETL_Metadata
GO
SELECT COUNT(*) FROM test.TargetStage

/* Pipeline Parameters, provided by Pipeline itself */
DECLARE @PipelineName VARCHAR(150) = 'Test Incremental Extraction SQL Pipeline',
		@PipelineType VARCHAR(150) = 'Extraction',
		@FullLoad BIT = 0,
		@NextWaterMark VARCHAR(50),
		@WaterMark VARCHAR(50),
		@Inserts INT,
		@TotalRows INT,
		@RunTimeID BIGINT,
		@PipelineID INT,
		@Active BIT
SET @NextWaterMark = CHANGE_TRACKING_CURRENT_VERSION()

DECLARE @SourceDatabaseName VARCHAR(100) = 'SourceDatabase',
		@SourceSchemaName VARCHAR(100) = 'test',
		@SourceTableName VARCHAR(100) = 'SourceTable',
		@SourceFullAddress VARCHAR(200)
SET @SourceFullAddress = CONCAT_WS('.',  @SourceDatabaseName, @SourceSchemaName, @SourceTableName)
DECLARE @SourceAddressType VARCHAR(150) = 'Database'

DECLARE @TargetDatabaseName VARCHAR(100) = 'ETL_Metadata',
		@TargetSchemaName VARCHAR(100) = 'test',
		@TargetTableName VARCHAR(100) = 'TargetStage',
		@TargetFullAddress VARCHAR(200)
SET @TargetFullAddress = CONCAT_WS('.',@TargetDatabaseName,@TargetSchemaName,@TargetTableName)
DECLARE @TargetAddressType VARCHAR(150) = 'Database'

EXEC etl.PipelineActive_S @PipelineName = @PipelineName,
						 @Active = @Active

IF @Active = 1
BEGIN
	/* First DAG */
	EXEC etl.RunTime_I @PipelineName = @PipelineName,
								@PipelineType = @PipelineType,
								@FullLoad = @FullLoad,
								@SourceFullAddress = @SourceFullAddress,
								@SourceAddressType = @SourceAddressType,
								@TargetFullAddress = @TargetFullAddress,
								@TargetAddressType = @TargetAddressType,
								@NextWaterMark = @NextWaterMark,
								@WaterMark = @WaterMark OUTPUT,
								@RunTimeID = @RunTimeID OUTPUT,
								@PipelineID = @PipelineID OUTPUT
	/* Extract Data */
	IF @FullLoad = 1
	BEGIN

		SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
		TRUNCATE TABLE test.TargetPublish
		INSERT INTO test.TargetPublish (
			EventID, 
			EventValue, 
			EventReason, 
			CreatedDate, 
			RowHash, 
			RunTimeID
		)
		SELECT
		CAST(st.EventID AS BIGINT) AS EventID,
		CAST(st.EventValue AS DECIMAL(17,2)) AS EventValue,
		CAST(st.EventReason AS VARCHAR(50)) AS EventReason,
		CAST(st.CreatedDate AS DATETIMEOFFSET(2)) AS CreatedDate,
		CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', CONCAT_WS('|',
			ISNULL(CONVERT(VARCHAR(50), st.EventID), ''),
			ISNULL(CONVERT(VARCHAR(50), st.EventValue), ''),
			ISNULL(CONVERT(VARCHAR(50), st.EventReason), ''),
			ISNULL(CONVERT(VARCHAR(50), st.CreatedDate), '')
		))) AS RowHash,
		@RunTimeID AS RunTimeID
		FROM test.SourceTable AS st WITH (TABLOCK)
		ORDER BY st.EventID
		SET @Inserts = @@ROWCOUNT
		SET TRANSACTION ISOLATION LEVEL READ COMMITTED
	
	END
	ELSE
	BEGIN

		SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
		INSERT INTO test.TargetStage (
			EventID, 
			EventValue, 
			EventReason, 
			CreatedDate, 
			CTOperation, 
			RowHash, 
			RunTimeID,
			CTOperationContext
		)
		SELECT
		CAST(ct.EventID AS BIGINT) AS EventID,
		CAST(st.EventValue AS DECIMAL(17,2)) AS EventValue,
		CAST(st.EventReason AS VARCHAR(50)) AS EventReason,
		CAST(st.CreatedDate AS DATETIMEOFFSET(2)) AS CreatedDate,
		ct.SYS_CHANGE_OPERATION AS CTOperation,
		CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', CONCAT_WS('|',
			ISNULL(CONVERT(VARCHAR(50), ct.EventID), ''),
			ISNULL(CONVERT(VARCHAR(50), st.EventValue), ''),
			ISNULL(CONVERT(VARCHAR(50), st.EventReason), ''),
			ISNULL(CONVERT(VARCHAR(50), st.CreatedDate), '')
		))) AS RowHash,
		@RunTimeID AS RunTimeID,
		CAST(ct.SYS_CHANGE_CONTEXT AS VARCHAR(128)) AS CTOperationContext
		FROM CHANGETABLE(CHANGES test.SourceTable, @WaterMark) AS ct
		LEFT JOIN test.SourceTable AS st
			ON st.EventID = ct.EventID
		WHERE ISNULL(ct.SYS_CHANGE_CONTEXT, CAST('' AS VARBINARY(128))) != CAST('Ignore' AS VARBINARY(128)) /* Can ignore certain records marked */
		SET @Inserts = @@ROWCOUNT
		SET TRANSACTION ISOLATION LEVEL READ COMMITTED

	END
	SET @TotalRows = @Inserts
	SELECT @Inserts AS Inserts

	/* Last DAG */
	EXEC etl.RunTime_U @RunTimeID = @RunTimeID,
								@Inserts = @Inserts,
								@TotalRows = @TotalRows
END

/* Validation */
SELECT * FROM etl.Pipeline WHERE PipelineID = @PipelineID
SELECT * FROM etl.RunTime WHERE RunTimeID = @RunTimeID

SELECT * FROM etl.Address

SELECT COUNT(*) FROM test.TargetStage

SELECT * FROM test.TargetStage WHERE CTOperation = 'U'