SET NOCOUNT ON;
THROW 51000, 'Make sure to designate the right database and then comment this out', 1;

/* Create etl Schema */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'etl')
BEGIN
	EXEC('CREATE SCHEMA etl AUTHORIZATION dbo')
END

/* Create Tables */
SET NOCOUNT ON 
DROP TABLE IF EXISTS etl.AuditColumnLog
DROP TABLE IF EXISTS etl.AuditCustomLog
DROP TABLE IF EXISTS etl.TimelinessRunTimeLog
DROP TABLE IF EXISTS etl.RunTime
CREATE TABLE etl.RunTime (
	ParentRunTimeID BIGINT NULL,
	RunTimeID BIGINT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_etl_RunTime_RunTimeID PRIMARY KEY,
	PipelineID INT NOT NULL,
	PipelineStart DATETIMEOFFSET(2) NOT NULL,
	PipelineEnd DATETIMEOFFSET(2) NULL,
	RunTimeSeconds INT NULL,
	Completed BIT NOT NULL
		CONSTRAINT DF_etl_RunTime_Completed DEFAULT(0),
	Inserts INT NULL,
	Updates INT NULL,
	SoftDeletes INT NULL,
	TotalRows INT NULL,
	FullLoad BIT NULL,
	WaterMark VARCHAR(50) NULL,
	NextWaterMark VARCHAR(50) NULL
)
CREATE NONCLUSTERED INDEX IX_etl_RunTime_PipelineID
	ON etl.RunTime(PipelineID)
CREATE NONCLUSTERED INDEX IX_etl_RunTime_PipelineEnd
	ON etl.RunTime(PipelineEnd) WHERE (PipelineEnd IS NOT NULL)

DROP TABLE IF EXISTS etl.AuditColumnRule
DROP TABLE IF EXISTS etl.AuditCustomRule
DROP TABLE IF EXISTS etl.TimelinessRunTimeLog
DROP TABLE IF EXISTS etl.Pipeline
CREATE TABLE etl.Pipeline (
	PipelineID INT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_etl_Pipeline_PipelineID PRIMARY KEY,
	PipelineName VARCHAR(150) NOT NULL,
	PipelineTypeID TINYINT NOT NULL,
	WaterMark VARCHAR(50) NULL,
	NextWaterMark VARCHAR(50) NULL,
	SourceAddressID INT NULL,
	TargetAddressID INT NULL ,
	LastTargetInsert DATETIMEOFFSET(2) NULL,
	LastTargetUpdate DATETIMEOFFSET(2) NULL,
	LastTargetDelete DATETIMEOFFSET(2) NULL,
	PipelineArgs VARCHAR(4000) NULL,
	TimelyNumber INT NULL,
	TimelyDatePart VARCHAR(20) NULL
		CONSTRAINT CK_etl_Pipeline_TimelyDatePart CHECK(TimelyDatePart IN (NULL, 'MINUTE', 'HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR')),
	MuteTimelyCheck BIT NOT NULL
		CONSTRAINT DF_etl_Pipeline_MuteTimelyCheck DEFAULT (0),
	LoadLineage BIT NOT NULL
		CONSTRAINT DF_etl_Pipeline_LoadLineage DEFAULT(0),
	Active BIT NOT NULL
		CONSTRAINT DF_etl_Pipeline_Active DEFAULT(1)
)
CREATE UNIQUE NONCLUSTERED INDEX IX_etl_Pipeline_PipelineName
	ON etl.Pipeline (PipelineName) INCLUDE (LoadLineage, Active)
CREATE NONCLUSTERED INDEX IX_etl_Pipeline_MuteTimelyCheck
	ON etl.Pipeline (PipelineTypeID)


ALTER TABLE etl.RunTime
	ADD CONSTRAINT FK_etl_RunTime_PipelineID FOREIGN KEY (PipelineID)
		REFERENCES etl.Pipeline (PipelineID)

DROP TABLE IF EXISTS etl.PipelineType
CREATE TABLE etl.PipelineType (
	PipelineTypeID TINYINT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_etl_PipelineType_PipelineTypeID PRIMARY KEY,
	PipelineType VARCHAR(150) NOT NULL,
	TimelyNumber INT NULL,
	TimelyDatePart VARCHAR(20) NULL
		CONSTRAINT CK_etl_PipelineType_TimelyDatePart CHECK(TimelyDatePart IN (NULL, 'MINUTE', 'HOUR', 'DAY', 'WEEK', 'MONTH', 'YEAR')),
	MuteTimelyCheck BIT NOT NULL
)
CREATE UNIQUE NONCLUSTERED INDEX IX_etl_PipelineType_PipelineType
	ON etl.PipelineType (PipelineType)

INSERT INTO etl.PipelineType (PipelineType, TimelyNumber, TimelyDatePart, MuteTimelyCheck)
VALUES 
('Controller', NULL, NULL, 1),
('Extraction', 7, 'DAY', 0), 
('Audit', NULL, NULL, 1),
('Publish', 7, 'DAY', 0),
('Transformation', 7, 'DAY', 0),
('Fact', 25, 'HOUR', 0),
('Dimension', 25, 'HOUR', 0)


ALTER TABLE etl.Pipeline
	ADD CONSTRAINT FK_etl_Pipeline_PipelineTypeID FOREIGN KEY (PipelineTypeID)
		REFERENCES etl.PipelineType (PipelineTypeID)

DROP TABLE IF EXISTS etl.PipelineUser
CREATE TABLE etl.PipelineUser (
	PipelineUserID INT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_etl_PipelineUser_PipelineUserID PRIMARY KEY,
	UserName VARCHAR(150)
)
CREATE UNIQUE NONCLUSTERED INDEX IX_etl_PipelineUser_UserName
	ON etl.PipelineUser (UserName)

DROP TABLE IF EXISTS etl.AddressDependency
DROP TABLE IF EXISTS etl.Address
CREATE TABLE etl.Address (
	AddressID INT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_etl_Address_AddressID PRIMARY KEY,
	FullAddress VARCHAR(150) NOT NULL,
	AddressTypeID TINYINT NOT NULL,
	DatabaseName VARCHAR(50) NULL,
	SchemaName VARCHAR(50) NULL,
	TableName VARCHAR(50) NULL,
	PrimaryKey VARCHAR(50) NULL,
	Deprecated BIT NOT NULL
		CONSTRAINT DF_etl_Address_Deprecated DEFAULT(0)
)
CREATE UNIQUE NONCLUSTERED INDEX IX_etl_Address_FullAddress
	ON etl.Address (FullAddress)

DROP TABLE IF EXISTS etl.AddressType
CREATE TABLE etl.AddressType (
	AddressTypeID TINYINT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_etl_AddressType_AddressTypeID PRIMARY KEY,
	AddressType VARCHAR(150) NOT NULL
)
CREATE UNIQUE NONCLUSTERED INDEX IX_etl_AddressType_AddressType
	ON etl.AddressType (AddressType)
INSERT INTO etl.AddressType (AddressType)
VALUES
('Database'),
('File'),
('Report'),
('API')

ALTER TABLE etl.Address
	ADD CONSTRAINT FK_etl_Address_AddressTypeID FOREIGN KEY (AddressTypeID)
		REFERENCES etl.AddressType (AddressTypeID)

ALTER TABLE etl.Pipeline
	ADD CONSTRAINT FK_etl_Pipeline_SourceAddressID FOREIGN KEY (SourceAddressID)
		REFERENCES etl.Address (AddressID)
ALTER TABLE etl.Pipeline
	ADD CONSTRAINT FK_etl_Pipeline_TargetAddressID FOREIGN KEY (TargetAddressID)
		REFERENCES etl.Address (AddressID)

DROP TABLE IF EXISTS etl.AuditType
CREATE TABLE etl.AuditType (
	AuditTypeID TINYINT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_etl_AuditType_AuditTypeID PRIMARY KEY,
	AuditType VARCHAR(150) NOT NULL
)
CREATE UNIQUE NONCLUSTERED INDEX IX_etl_AuditType_AuditType
	ON etl.AuditType (AuditType)
INSERT INTO etl.AuditType (AuditType)
VALUES 
('Completeness'),
('Accuracy'),
('Timeliness'),
('Custom')

DROP TABLE IF EXISTS etl.AuditColumnRule
CREATE TABLE etl.AuditColumnRule (
	AuditColumnRuleID INT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_etl_AuditColumnRule_AuditColumnRuleID PRIMARY KEY,
	PipelineID INT NOT NULL,
	AuditTypeID TINYINT NOT NULL,
	AuditColumn VARCHAR(50) NOT NULL,
	MinimumBound VARCHAR(50) NULL,
	MaximumBound VARCHAR(50) NULL,
	LookbackDays INT NOT NULL,
	StdDeviationFactor DECIMAL(17,2) NOT NULL,
	InfoAlert BIT NOT NULL,
	ErrorAlert BIT NOT NULL,
	RuleStartDate DATE NOT NULL,
	RuleEndDate DATE NOT NULL,
	LastAuditColumnLogID BIGINT NULL,
	Active BIT NOT NULL
		CONSTRAINT DF_etl_AuditColumnRule_Active DEFAULT(1)
)
ALTER TABLE etl.AuditColumnRule
	ADD CONSTRAINT FK_etl_AuditColumnRule_PipelineID FOREIGN KEY (PipelineID)
		REFERENCES etl.Pipeline (PipelineID)
ALTER TABLE etl.AuditColumnRule
	ADD CONSTRAINT FK_etl_AuditColumnRule_AuditTypeID FOREIGN KEY (AuditTypeID)
		REFERENCES etl.AuditType (AuditTypeID)
CREATE NONCLUSTERED INDEX IX_etl_AuditColumnRule_PipelineID_AuditTypeID_Active
	ON etl.AuditColumnRule (PipelineID, AuditTypeID, Active)

DROP TABLE IF EXISTS etl.AuditColumnLog
CREATE TABLE etl.AuditColumnLog (
	AuditColumnLogID BIGINT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_etl_AuditColumnLog_AuditColumnLogID PRIMARY KEY,
	RunTimeID BIGINT NOT NULL,
	AuditColumnRuleID INT NOT NULL,
	DateRecorded DATE NOT NULL,
	HourRecorded TINYINT NOT NULL,
	RecordCount BIGINT NOT NULL, 
	RecordViolationCount BIGINT NOT NULL,
	ResultScore DECIMAL(20,4) NOT NULL,
	Threshold DECIMAL(20,4) NULL,
	Success BIT NULL,
	FullLoad BIT NOT NULL,
	ChunkOut BIT NOT NULL
)
ALTER TABLE etl.AuditColumnLog
	ADD CONSTRAINT FK_etl_AuditColumnLog_RunTimeID FOREIGN KEY (RunTimeID)
		REFERENCES etl.RunTime (RunTimeID)
ALTER TABLE etl.AuditColumnLog
	ADD CONSTRAINT FK_etl_AuditColumnLog_AuditColumnRuleID FOREIGN KEY (AuditColumnRuleID)
		REFERENCES etl.AuditColumnRule (AuditColumnRuleID)
CREATE NONCLUSTERED INDEX IX_etl_AuditColumnLog_AuditColumnRuleID
	ON etl.AuditColumnLog (AuditColumnRuleID, HourRecorded, DateRecorded)
		INCLUDE (ResultScore)
CREATE UNIQUE NONCLUSTERED INDEX IX_etl_AuditColumnLog_RunTimeID_AuditColumnRuleID
	ON etl.AuditColumnLog (RunTimeID, AuditColumnRuleID)

DROP TABLE IF EXISTS etl.AddressDependency
CREATE TABLE etl.AddressDependency (
	AddressDependencyID INT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_etl_AddressDependency_AddressDependencyID PRIMARY KEY,
	SourceAddressID INT NOT NULL,
	TargetAddressID INT NOT NULL
)
CREATE UNIQUE NONCLUSTERED INDEX IX_etl_AddressDependency_TargetAddresses
	ON etl.AddressDependency (TargetAddressID, SourceAddressID)
CREATE UNIQUE NONCLUSTERED INDEX IX_etl_AddressDependency_SourceAddresses
	ON etl.AddressDependency (SourceAddressID, TargetAddressID)
ALTER TABLE etl.AddressDependency
	ADD CONSTRAINT FK_etl_AddressDependency_SourceAddressID FOREIGN KEY (SourceAddressID)
		REFERENCES etl.Address (AddressID)
ALTER TABLE etl.AddressDependency
	ADD CONSTRAINT FK_etl_AddressDependency_TargetAddressID FOREIGN KEY (TargetAddressID)
		REFERENCES etl.Address (AddressID)

DROP TABLE IF EXISTS etl.Deployment
CREATE TABLE etl.Deployment (
	Ticket VARCHAR(20) NOT NULL
		CONSTRAINT PK_etl_Deployment_Ticket PRIMARY KEY
)

DROP TABLE IF EXISTS etl.TimelinessRunTimeLog
CREATE TABLE etl.TimelinessRunTimeLog (
	TimelinessRunTimeLogID BIGINT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_etl_TimelinessRunTimeLog_TimelinessRunTimeLogID PRIMARY KEY,
	RunTimeID BIGINT NOT NULL,
	PipelineID INT NOT NULL,
	RunTimeSeconds BIGINT NOT NULL,
	SecondsThreshold INT NOT NULL
)
CREATE UNIQUE INDEX IX_etl_TimelinessRunTimeLog_RunTimeID
	ON etl.TimelinessRunTimeLog (RunTimeID)
ALTER TABLE etl.TimelinessRunTimeLog
	ADD CONSTRAINT FK_etl_TimelinessRunTimeLog_RunTimeID FOREIGN KEY (RunTimeID)
		REFERENCES etl.RunTime (RunTimeID)
ALTER TABLE etl.TimelinessRunTimeLog
	ADD CONSTRAINT FK_etl_TimelinessRunTimeLog_PipelineID FOREIGN KEY (PipelineID)
		REFERENCES etl.Pipeline (PipelineID)

DROP TABLE IF EXISTS etl.AuditCustomRule
CREATE TABLE etl.AuditCustomRule (
	AuditCustomRuleID INT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_etl_AuditCustomRule_AuditCustomRuleID PRIMARY KEY,
	PipelineID INT NOT NULL,
	CustomSQL NVARCHAR(MAX),
	CustomAlertMessage VARCHAR(400),
	RuleStartDate DATE NOT NULL,
	RuleEndDate DATE NOT NULL,
	Active BIT NOT NULL
		CONSTRAINT DF_etl_AuditCustomRule_Active DEFAULT (1),
	InfoAlert BIT NOT NULL,
	ErrorAlert BIT NOT NULL
)
ALTER TABLE etl.AuditCustomRule
	ADD CONSTRAINT FK_etl_AuditCustomRule_PipelineID FOREIGN KEY (PipelineID)
		REFERENCES etl.Pipeline (PipelineID)
CREATE NONCLUSTERED INDEX IX_etl_AuditCustomRule_PipelineID_Active
	ON etl.AuditCustomRule (PipelineID, Active)

DROP TABLE IF EXISTS etl.AuditCustomLog
CREATE TABLE etl.AuditCustomLog (
	AuditCustomLogID BIGINT IDENTITY(1,1) NOT NULL
		CONSTRAINT PK_etl_AuditCustomLog_AuditCustomLogID PRIMARY KEY,
	RunTimeID BIGINT NOT NULL,
	AuditCustomRuleID INT NOT NULL,
	CustomQueryStart DATETIMEOFFSET(2) NOT NULL,
	CustomQueryEnd DATETIMEOFFSET(2) NULL,
	RunTimeSeconds INT NULL,
	Completed BIT NOT NULL
		CONSTRAINT DF_etl_AuditCustomLog_Completed DEFAULT(0),
	Success BIT NULL
)
CREATE UNIQUE INDEX IX_etl_AuditCustomLog_RunTimeID_AuditCustomRuleID
	ON etl.AuditCustomLog (RunTimeID, AuditCustomRuleID)
ALTER TABLE etl.AuditCustomLog
	ADD CONSTRAINT FK_etl_AuditCustomLog_RunTimeID FOREIGN KEY (RunTimeID)
		REFERENCES etl.RunTime (RunTimeID)
ALTER TABLE etl.AuditCustomLog
	ADD CONSTRAINT FK_etl_AuditCustomLog_AuditCustomRuleID FOREIGN KEY (AuditCustomRuleID)
		REFERENCES etl.AuditCustomRule (AuditCustomRuleID)

DROP TABLE IF EXISTS etl.DeprecationDeclared
CREATE TABLE etl.DeprecationDeclared (
	DeprecationDeclaredID BIGINT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_etl_DeprecationDeclared_DeprecationDeclaredID PRIMARY KEY,
	AddressID INT NOT NULL,
	ID INT NOT NULL,
	IDType VARCHAR(150) NOT NULL,
	IDName VARCHAR(150) NULL,
	MarkedForDeprecation DATETIMEOFFSET(2) NOT NULL,
	TicketsCreated BIT NOT NULL
		CONSTRAINT DF_etl_DeprecationDeclared_TicketsCreated DEFAULT(0)
)
CREATE UNIQUE INDEX IX_etl_DeprecationDeclared_AddressID_ID_IDType
	ON etl.DeprecationDeclared (AddressID, ID, IDType)
CREATE NONCLUSTERED INDEX IX_etl_DeprecationDeclared_MarkedForDeprecation
	ON etl.DeprecationDeclared (MarkedForDeprecation) INCLUDE (TicketsCreated)


/* Create Types */
DROP PROCEDURE IF EXISTS etl.AddressDependency_DI
DROP TYPE IF EXISTS etl.SourceAddresses
CREATE TYPE etl.SourceAddresses AS TABLE (
	FullAddress VARCHAR(150) NOT NULL,
	AddressType VARCHAR(150) NOT NULL
)

/* Create Stored Procedures */
DROP PROCEDURE IF EXISTS etl.Address_SI
GO
CREATE PROCEDURE [etl].[Address_SI] @FullAddress VARCHAR(150),
								   @AddressType VARCHAR(150),
								   @AddressID INT OUTPUT
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	SELECT @AddressID = AddressID
	FROM etl.Address
	WHERE FullAddress = @FullAddress

	/* If address cannot be found, create it */
	IF @AddressID IS NULL
	BEGIN
		DECLARE @AddressTypeID TINYINT,
				@DatabaseName VARCHAR(50) = NULL,
				@SchemaName VARCHAR(50) = NULL,
				@TableName VARCHAR(50) = NULL

		SELECT @AddressTypeID = AddressTypeID
		FROM etl.AddressType
		WHERE AddressType = @AddressType

		IF @AddressTypeID IS NULL
		BEGIN
			DECLARE @ErrorMessage VARCHAR(100) = 'Unable to find AddressType: ' + @AddressType + '';
			THROW 51000, @ErrorMessage, 1;
		END

		IF @AddressType = 'Database'
		BEGIN
			SET @DatabaseName = PARSENAME(@FullAddress, 3)
			SET @SchemaName = PARSENAME(@FullAddress, 2)
			SET @TableName = PARSENAME(@FullAddress, 1)
		END

		INSERT INTO etl.Address (FullAddress, AddressTypeID, DatabaseName, SchemaName, TableName)
		VALUES (@FullAddress, @AddressTypeID, @DatabaseName, @SchemaName, @TableName)

		SET @AddressID = SCOPE_IDENTITY()
	END
END
GO

DROP PROCEDURE IF EXISTS etl.AddressDependency_DI
GO
CREATE PROCEDURE [etl].[AddressDependency_DI] @TargetFullAddress VARCHAR(150),
											 @TargetAddressType VARCHAR(150),
											 @SourceAddresses etl.SourceAddresses READONLY
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DECLARE @TargetAddressID INT,
			@SourceFullAddress VARCHAR(150),
			@SourceAddressType VARCHAR(150),
			@SourceAddressID INT

	DECLARE @SourceIDs TABLE (SourceAddressID INT)

	/* Insert source addresses that we don't have yet */
	DECLARE address_cursor CURSOR FAST_FORWARD FOR
		SELECT
		FullAddress,
		AddressType
		FROM @SourceAddresses
		ORDER BY FullAddress

	OPEN address_cursor
	FETCH NEXT FROM address_cursor INTO
	@SourceFullAddress, @SourceAddressType
	
	WHILE @@FETCH_STATUS = 0
	BEGIN
		/* This is required to have the variable be overwritten correctly */
		SET @SourceAddressID = NULL 

		EXEC etl.Address_SI @FullAddress = @SourceFullAddress,
							@AddressType = @SourceAddressType,
							@AddressID = @SourceAddressID OUTPUT

		INSERT INTO @SourceIDs (SourceAddressID)
		VALUES (@SourceAddressID)

		FETCH NEXT FROM address_cursor INTO
		@SourceFullAddress, @SourceAddressType
	END

	CLOSE address_cursor
	DEALLOCATE address_cursor

	/* Grab target address */
	SELECT @TargetAddressID = AddressID
	FROM etl.Address
	WHERE FullAddress = @TargetFullAddress

	/* If target address can't be found, create it */
	IF @TargetAddressID IS NULL
	BEGIN
		EXEC etl.Address_SI @FullAddress = @TargetFullAddress,
							 @AddressType = @TargetAddressType,
							 @AddressID = @TargetAddressID OUTPUT
	END

	/* Create table so we can cross join for combinations */
	DECLARE @TargetAddressTable TABLE (TargetAddressID INT)
	INSERT INTO @TargetAddressTable (TargetAddressID) VALUES (@TargetAddressID)

	/* Create combinations */
	DROP TABLE IF EXISTS #TableDependency
	CREATE TABLE #TableDependency (
		SourceAddressID INT NOT NULL,
		TargetAddressID INT NOT NULL
	)
	INSERT INTO #TableDependency (SourceAddressID, TargetAddressID)
	SELECT
	s.SourceAddressID,
	t.TargetAddressID
	FROM @SourceIDs AS s
	CROSS JOIN @TargetAddressTable AS t

	/* Delete any source addresses that are not found in inputted list */
	DELETE td
	FROM etl.AddressDependency AS td
	WHERE td.TargetAddressID = @TargetAddressID
		AND NOT EXISTS (
			SELECT 1/0
			FROM #TableDependency AS d
			WHERE d.SourceAddressID = td.SourceAddressID
		)

	/* Input given combinations into dependency table */
	INSERT INTO etl.AddressDependency (SourceAddressID, TargetAddressID)
	SELECT
	SourceAddressID,
	TargetAddressID
	FROM #TableDependency AS td
	WHERE NOT EXISTS (
		SELECT 1/0
		FROM etl.AddressDependency AS ad
		WHERE ad.SourceAddressID = td.SourceAddressID
			AND ad.TargetAddressID = td.TargetAddressID
		)

END
GO


DROP PROCEDURE IF EXISTS etl.AuditAccuracy_I
GO
CREATE PROCEDURE etl.AuditAccuracy_I @ParentRunTimeID BIGINT,
									 @RunTimeID BIGINT,
									 @PipelineID INT,
									 @FullLoad BIT,
									 @Debug BIT = 0
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DECLARE @Timestamp DATETIMEOFFSET(2),
			@DateRecorded DATE,
			@HourRecorded TINYINT,
			@ChunkOut BIT = 0,
			@AuditType VARCHAR(150) = 'Accuracy',
			@AuditTypeID TINYINT,
			@SQLQuery NVARCHAR(MAX),
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

	/* Use pipeline start of the audit pipeline to determine date/hour comparisons */
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
			@RecordCount INT

	/* Each list needs unique tag added to prevent identity clash */
	SET @AggColumnList = (SELECT STRING_AGG(AuditColumn + '#Violation', ', ') WITHIN GROUP (ORDER BY AuditColumnRuleID) FROM #AccuracyRules)
	SET @CountColumnList = (SELECT STRING_AGG(AuditColumn + '#Count', ', ') WITHIN GROUP (ORDER BY AuditColumnRuleID) FROM #AccuracyRules)	
	
	SET @Count_List = (SELECT STRING_AGG('COUNT(' + AuditColumn + ') AS ' + AuditColumn + '#Count', ', ') WITHIN GROUP (ORDER BY AuditColumnRuleID) FROM #AccuracyRules)

	DECLARE @ColumnName VARCHAR(50), @MinimumBound VARCHAR(50), @MaximumBound VARCHAR(50)
	DECLARE accuracy_cursor CURSOR FOR
		SELECT AuditColumn, MinimumBound, MaximumBound
		FROM #AccuracyRules 
		WHERE Complete = 0
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
		SET Complete = 1
		WHERE AuditColumn = @ColumnName

		FETCH NEXT FROM accuracy_cursor INTO
		@ColumnName, @MinimumBound, @MaximumBound

	END

	CLOSE accuracy_cursor
	DEALLOCATE accuracy_cursor

	DROP TABLE IF EXISTS #Results
	CREATE TABLE #Results (
		ColumnName VARCHAR(50),
		RecordCount BIGINT,
		RecordViolationCount BIGINT
	)
	IF @FullLoad = 0
	BEGIN
		/* Only check against parent's last run time */
		SET @SQLQuery = '
		DROP TABLE IF EXISTS #temp
		;WITH CTE AS (
		SELECT
		' + @PrimaryKey + '
		FROM ' + @TargetTable + '
		WHERE RunTimeID = ' + CAST(@ParentRunTimeID AS VARCHAR(50)) + '
		)
		SELECT
		{agg_list}
		{count_list}
		INTO #temp
		FROM ' + @TargetTable + '
		INNER JOIN CTE
			ON CTE.' + @PrimaryKey + ' = ' + @TargetTable + '.' + @PrimaryKey + '
	
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

		INSERT INTO #Results (ColumnName, RecordCount, RecordViolationCount)
		SELECT
		f.ColumnName,
		f.RecordCount,
		s.RecordViolationCount
		FROM #first AS f
		INNER JOIN #second AS s
			ON s.ColumnName = f.ColumnName'
	END
	ELSE
	BEGIN
		/* Double unpivot and join back query string */
		SET @SQLQuery = '
		DROP TABLE IF EXISTS #temp
		SELECT
		{agg_list}
		{count_list}
		INTO #temp
		FROM ' + @TargetTable + '
	
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

		INSERT INTO #Results (ColumnName, RecordCount, RecordViolationCount)
		SELECT
		f.ColumnName,
		f.RecordCount,
		s.RecordViolationCount
		FROM #first AS f
		INNER JOIN #second AS s
			ON s.ColumnName = f.ColumnName'
	END

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
		SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
		EXEC sys.sp_executesql @SQLQuery
		SET TRANSACTION ISOLATION LEVEL READ COMMITTED
	END

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

	/* Use pipeline start of the audit pipeline to determine date/hour comparisons */
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
	IF @FullLoad = 0 /* If incremental, only check parent run time's data */
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
	IF @FullLoad=0 /* If incremental, only check parent run time's data */
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
GO


DROP PROCEDURE IF EXISTS etl.AuditCompleteness_I
GO
CREATE PROCEDURE etl.AuditCompleteness_I @ParentRunTimeID BIGINT = NULL,
										 @RunTimeID BIGINT,
										 @PipelineID INT,
										 @FullLoad BIT,
										 @Debug BIT = 0
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DECLARE @Timestamp DATETIMEOFFSET(2),
			@DateRecorded DATE,
			@HourRecorded TINYINT,
			@ChunkOut BIT = 0,
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

	/* Use pipeline start of the audit pipeline to determine date/hour comparisons */
	SELECT @Timestamp = PipelineStart
	FROM etl.RunTime
	WHERE RunTimeID = @RunTimeID

	SET @DateRecorded = CAST(@Timestamp AS DATE)
	SET @HourRecorded = DATEPART(HOUR, @Timestamp)

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
	a.AuditColumn
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
			@RecordCount INT

	SELECT
	@ColumnList = STRING_AGG(AuditColumn, ',') WITHIN GROUP (ORDER BY AuditColumnRuleID),
	@Agg_List = STRING_AGG( 'SUM( CASE WHEN ' + AuditColumn + ' IS NULL THEN 1 ELSE 0 END) AS ' + AuditColumn + '', ',') WITHIN GROUP (ORDER BY AuditColumnRuleID)
	FROM #CompletenessRules

	DECLARE @SQLQuery NVARCHAR(MAX)

	DROP TABLE IF EXISTS #Results
	CREATE TABLE #Results (
		RecordCount BIGINT,
		ColumnName VARCHAR(50),
		RecordViolationCount BIGINT
	)

	IF @FullLoad=0
	BEGIN
		/* Only audit the last run time of the parent */
		SET @SQLQuery = '
		;WITH CTE AS (
		SELECT
		' + @PrimaryKey + '
		FROM ' + @TargetTable + '
		WHERE RunTimeID = ' + CAST(@ParentRunTimeID AS VARCHAR(50)) + '
		)

		INSERT INTO #Results (RecordCount, ColumnName, RecordViolationCount)
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
			ON CTE.' + @PrimaryKey + ' = ' + @TargetTable + '.' + @PrimaryKey + '
		) AS SubQuery
		UNPIVOT
		( RecordViolationCount FOR ColumnName IN (
		{column_list}
		)) AS pvt'
	END
	ELSE
	BEGIN
		/* simple unpivot */
		SET @SQLQuery = '
		INSERT INTO #Results (RecordCount, ColumnName, RecordViolationCount)
		SELECT
		RecordCount,
		ColumnName,
		RecordViolationCount
		FROM (
		SELECT
		COUNT(*) AS RecordCount,
		{agg_list}
		FROM ' + @TargetTable + '
		) AS SubQuery
		UNPIVOT
		( RecordViolationCount FOR ColumnName IN (
		{column_list}
		)) AS pvt'
	END

	/* inject constructed column list and agg list into string */
	SET @SQLQuery = REPLACE(@SQLQuery, '{agg_list}', @Agg_List)
	SET @SQLQuery = REPLACE(@SQLQuery, '{column_list}', @ColumnList)

	IF @Debug=1
	BEGIN
		PRINT @SQLQuery
	END
	ELSE
	BEGIN
		SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
		EXEC sys.sp_executesql @SQLQuery
		SET TRANSACTION ISOLATION LEVEL READ COMMITTED
	END

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

	/* Use pipeline start of the audit pipeline to determine date/hour comparisons */
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
	IF @FullLoad = 0 /* If incremental, only check parent run time's data */
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
	IF @FullLoad=0 /* If incremental, only check parent run time's data */
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
GO


DROP PROCEDURE IF EXISTS etl.AuditColumnLogCheck
GO
CREATE PROCEDURE etl.AuditColumnLogCheck @RunTimeID BIGINT,
										 @PipelineID INT,
										 @AuditType VARCHAR(150)
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DECLARE @Success BIT = 1,
			@PipelineStart DATETIMEOFFSET(2),
			@DateRecorded DATE,
			@HourRecorded TINYINT,
			@SamplesNeeded INT,
			@AuditTypeID TINYINT

	/* Cannot depend on specific ID, so seek on text */
	SELECT @AuditTypeID = AuditTypeID
	FROM etl.AuditType
	WHERE AuditType = @AuditType

	/* Using timestamp of audit pipeline for date/hour comparison */
	SELECT @PipelineStart = PipelineStart
	FROM etl.RunTime
	WHERE RunTimeID = @RunTimeID

	/* Grab required number of samples before alerting starts */
	SELECT @SamplesNeeded = ISNULL(JSON_VALUE(PipelineArgs, '$.info.audit.samples_needed'), 10)
	FROM etl.Pipeline
	WHERE PipelineID = @PipelineID

	SET @DateRecorded = CAST(@PipelineStart AS DATE)
	SET @HourRecorded = DATEPART(HOUR, @PipelineStart)

	/* Grab alert information from rules for audit type */
	DROP TABLE IF EXISTS #Alerts
	;WITH CTE AS (
	SELECT
	AuditColumnRuleID
	FROM etl.AuditColumnRule
	WHERE PipelineID = @PipelineID
		AND AuditTypeID = @AuditTypeID
		AND Active=1
	)
	SELECT
	acr.AuditColumnRuleID,
	acr.LookbackDays,
	acr.StdDeviationFactor,
	acr.InfoAlert,
	acr.ErrorAlert,
	acr.LastAuditColumnLogID,
	0 AS Complete
	INTO #Alerts
	FROM etl.AuditColumnRule AS acr
	INNER JOIN CTE
		ON CTE.AuditColumnRuleID = acr.AuditColumnRuleID

	DECLARE @AuditColumnRuleID INT,
			@LookbackDays INT,
			@StdDeviationFactor DECIMAL(17,2),
			@InfoAlert BIT,
			@ErrorAlert BIT,
			@LastAuditColumnLogID BIGINT

	DECLARE alert_cursor CURSOR FOR
		SELECT
		AuditColumnRuleID,
		LookbackDays,
		StdDeviationFactor,
		InfoAlert,
		ErrorAlert,
		LastAuditColumnLogID
		FROM #Alerts
		WHERE Complete=0
		ORDER BY AuditColumnRuleID

	OPEN alert_cursor
	FETCH NEXT FROM alert_cursor INTO
		@AuditColumnRuleID, 
		@LookbackDays,
		@StdDeviationFactor, 
		@InfoAlert, 
		@ErrorAlert, 
		@LastAuditColumnLogID

	DECLARE @Date DATE,
			@ChangeThreshold DECIMAL(20,4),
			@ResultScore DECIMAL(20,4),
			@Count INT,
			@Threshold DECIMAL(20,4)

	DROP TABLE IF EXISTS #Results 
	CREATE TABLE #Results (
		AuditColumnRuleID INT,
		AuditColumnLogID BIGINT,
		ResultScore DECIMAL(20,4),
		Threshold DECIMAL(20,4),
		Success BIT,
		InfoAlert BIT,
		ErrorAlert BIT
	)
				
	WHILE @@FETCH_STATUS = 0
	BEGIN
		
		/* Grab result score we're evaluating */
		SELECT @ResultScore = ResultScore
		FROM etl.AuditColumnLog
		where AuditColumnLogID = @LastAuditColumnLogID

		/* Set how far back to get samples */
		SET @Date = DATEADD(DAY, -(@LookbackDays), @DateRecorded)
		
		/* Calculate change threshold and number of samples, default is 1 standard deviation */
		SELECT @ChangeThreshold = ISNULL(STDEV(ResultScore) * @StdDeviationFactor, 0),
		       @Count = COUNT(ResultScore)
		FROM etl.AuditColumnLog
		WHERE AuditColumnLogID != @LastAuditColumnLogID
			AND AuditColumnRuleID = @AuditColumnRuleID
			AND HourRecorded = @HourRecorded
			AND DateRecorded >= @Date

		/* If a change threshold of .2, then anything below .8 should be out of norm */
		SET @Threshold = 1 - @ChangeThreshold
		IF @ResultScore < @Threshold
			SET @Success = 0

		/* If not enough run samples, not enough to evaluate */
		IF @Count <= @SamplesNeeded
			SET @Success = NULL

		INSERT INTO #Results (
		AuditColumnRuleID,
		AuditColumnLogID,
		ResultScore,
		Threshold,
		Success,
		InfoAlert,
		ErrorAlert
		)
		VALUES (
		@AuditColumnRuleID,
		@LastAuditColumnLogID,
		@ResultScore,
		@Threshold,
		@Success,
		@InfoAlert,
		@ErrorAlert
		)

		/* Update log with evaluation result */
		UPDATE etl.AuditColumnLog
		SET Threshold = @Threshold,
			Success = @Success
		WHERE AuditColumnLogID = @LastAuditColumnLogID

		UPDATE #Alerts
		SET Complete=1
		WHERE AuditColumnRuleID = @AuditColumnRuleID

		FETCH NEXT FROM alert_cursor INTO
			@AuditColumnRuleID, 
			@LookbackDays,
			@StdDeviationFactor, 
			@InfoAlert, 
			@ErrorAlert, 
			@LastAuditColumnLogID

	END

	CLOSE alert_cursor
	DEALLOCATE alert_cursor

	/* Email alerts */
	DECLARE @Message VARCHAR(4000)
	IF EXISTS (SELECT 1/0 FROM #Results WHERE Success = 0 AND InfoAlert = 1)
	BEGIN

		SELECT @Message = MAX(CONCAT_WS('.', ap.DatabaseName, ap.SchemaName, ap.TableName)) + 
		' - failed info checks on these columns: ' + CHAR(13) + 
		STRING_AGG('' + @AuditType + ': ' + acr.AuditColumn, CHAR(13))
		FROM #Results AS r
		INNER JOIN etl.AuditColumnRule AS acr
			ON acr.AuditColumnRuleID = r.AuditColumnRuleID
		INNER JOIN etl.Pipeline AS p
			ON p.PipelineID = acr.PipelineID
		INNER JOIN etl.Address AS ap
			ON ap.AddressID = p.TargetAddressID
		WHERE r.Success = 0
			AND r.InfoAlert = 1

		/* TODO: Configure Database Mail */
		PRINT @Message

	END
	IF EXISTS (SELECT 1/0 FROM #Results WHERE Success = 0 AND ErrorAlert = 1)
	BEGIN
		SELECT @Message = MAX(CONCAT_WS('.', ap.DatabaseName, ap.SchemaName, ap.TableName)) + 
		' - failed error checks on these columns: ' + CHAR(13) + 
		STRING_AGG('' + @AuditType + ': ' + acr.AuditColumn, CHAR(13))
		FROM #Results AS r
		INNER JOIN etl.AuditColumnRule AS acr
			ON acr.AuditColumnRuleID = r.AuditColumnRuleID
		INNER JOIN etl.Pipeline AS p
			ON p.PipelineID = acr.PipelineID
		INNER JOIN etl.Address AS ap
			ON ap.AddressID = p.TargetAddressID
		WHERE r.Success = 0
			AND r.ErrorAlert = 1;


		/* TODO: Configure Database Mail */

		/* Throw alert to stop pipeline */
		THROW 51000, @Message, 1;

	END
END
GO


DROP PROCEDURE IF EXISTS etl.AuditColumnRule_I
GO
CREATE PROCEDURE [etl].[AuditColumnRule_I] @PipelineName VARCHAR(150),
									@AuditType VARCHAR(150),
									@DatabaseName VARCHAR(50) = NULL,
									@SchemaName VARCHAR(50) = NULL,
									@TableName VARCHAR(50) = NULL,
									@PrimaryKey VARCHAR(50),
									@AuditColumn VARCHAR(50),
									@MinimumBound VARCHAR(50) = NULL,
									@MaximumBound VARCHAR(50) = NULL,
									@LookbackDays INT = 30,
									@StdDeviationFactor DECIMAL(17,2) = 1.0,
									@InfoAlert BIT = NULL,
									@ErrorAlert BIT = NULL,
									@Override BIT = 0
											 
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DECLARE @ErrorMessage VARCHAR(200),
			@AuditTypeID TINYINT

	SELECT @AuditTypeID = AuditTypeID
	FROM etl.AuditType
	WHERE AuditType = @AuditType

	/* Check inputs and provide feedback for proper rule creation */
	IF @AuditTypeID IS NULL
	BEGIN
		SET @ErrorMessage = 'Unable to find AuditType: ' + @AuditType + '';
		THROW 51000, @ErrorMessage, 1;
	END
	IF (@AuditType = 'Accuracy' OR @AuditType = 'Completeness') AND (@PrimaryKey IS NULL OR @AuditColumn IS NULL)
	BEGIN
		SET @ErrorMessage = 'Accuracy and Completeness checks require the @PrimaryKey and @AuditColumn variables.';
		THROW 51000, @ErrorMessage, 1;
	END
	IF (@AuditType = 'Accuracy' OR @AuditType = 'Completeness') AND (@LookbackDays IS NULL OR @StdDeviationFactor IS NULL)
	BEGIN
		SET @ErrorMessage = 'Accuracy and Completeness checks require the @LookbackDays and @StdDeviationFactor variables.';
		THROW 51000, @ErrorMessage, 1;

	END
	IF @AuditType = 'Accuracy' AND @MinimumBound IS NULL AND @MaximumBound IS NULL 
	BEGIN
		SET @ErrorMessage = '@MinimumBound and @MaximumBound are both NULL, one is required for Accuracy check';
		THROW 51000, @ErrorMessage, 1;
	END
	IF @AuditType = 'Completeness' AND @Override = 1
	BEGIN
		SET @ErrorMessage = 'Cannot override a Completeness check';
		THROW 51000, @ErrorMessage, 1;
	END
	IF @AuditType = 'Completeness' AND (@MinimumBound IS NOT NULL OR @MaximumBound IS NOT NULL)
	BEGIN
		SET @ErrorMessage = '@MinimumBound and @MaximumBound are not utilized in Completeness checks';
		THROW 51000, @ErrorMessage, 1;
	END
	IF (@InfoAlert IS NULL OR @ErrorAlert IS NULL) OR (@InfoAlert=0 AND @ErrorAlert=0) OR (@InfoAlert=1 AND @ErrorAlert=1)
	BEGIN
		SET @ErrorMessage = 'Must declare @InfoAlert and @ErrorAlert variables, one must be active';
		THROW 51000, @ErrorMessage, 1;
	END
	IF @AuditType NOT IN ('Completeness', 'Accuracy')
	BEGIN
		SET @ErrorMessage = 'Column Audits currently only support Completeness and Accuracy checks';
		THROW 51000, @ErrorMessage, 1;
	END

	/* Begin script */
	DECLARE @TargetAddressID INT,
			@PipelineID INT,
			@RuleStartDate DATE = CAST(SYSDATETIMEOFFSET() AS DATE),
			@RuleEndDate DATE = '9999-12-31'

	/* Grab target address from pipeline */
	SELECT @TargetAddressID = TargetAddressID,
		   @PipelineID = PipelineID
	FROM etl.Pipeline AS p
	WHERE p.PipelineName = @PipelineName

	IF @PipelineID IS NULL
	BEGIN
		SET @ErrorMessage = 'Unable to find pipeline: ' + @PipelineName;
		THROW 51000, @ErrorMessage, 1;
	END

	/* update target address with inputs if provided, ensures table can be audited */
	UPDATE etl.Address
	SET DatabaseName = CASE WHEN @DatabaseName IS NOT NULL THEN @DatabaseName ELSE DatabaseName END,
		SchemaName = CASE WHEN @SchemaName IS NOT NULL THEN @SchemaName ELSE SchemaName END,
		TableName = CASE WHEN @TableName IS NOT NULL THEN @TableName ELSE TableName END,
		PrimaryKey = @PrimaryKey
	WHERE AddressID = @TargetAddressID

	/* Check if there is already an active rule for the column/audittype */
	IF @Override = 0
	BEGIN
	DECLARE @Message VARCHAR(1000)
	SET @Message = 'Active rule for audit column ''' + @AuditColumn + ''' with type ' + @AuditType + ' already exists.'
	IF EXISTS (
		SELECT 1/0
		FROM etl.AuditColumnRule 
		WHERE PipelineID = @PipelineID
			AND AuditTypeID = @AuditTypeID
			AND AuditColumn = @AuditColumn
			AND Active = 1
	)
		THROW 51000, @Message, 1;
	END
	ELSE
	BEGIN
		/* De-activate current rule if override = 1 */
		UPDATE etl.AuditColumnRule
		SET Active = 0,
			RuleEndDate = CAST(SYSDATETIMEOFFSET() AS DATE)
		WHERE PipelineID = @PipelineID
			AND AuditTypeID = @AuditTypeID
			AND AuditColumn = @AuditColumn
			AND Active = 1
	END

	/* Insert new active rule information */
	INSERT INTO etl.AuditColumnRule(
	PipelineID, 
	AuditTypeID,
	AuditColumn,
	MinimumBound,
	MaximumBound,
	LookbackDays,
	StdDeviationFactor,
	InfoAlert,
	ErrorAlert,
	RuleStartDate, 
	RuleEndDate, 
	Active
	)
	VALUES (
	@PipelineID,
	@AuditTypeID,
	@AuditColumn,
	@MinimumBound,
	@MaximumBound,
	@LookbackDays,
	@StdDeviationFactor,
	@InfoAlert,
	@ErrorAlert,
	@RuleStartDate,
	@RuleEndDate,
	1
	)

END
GO


DROP PROCEDURE IF EXISTS etl.AuditTimeliness_U
GO
CREATE PROCEDURE etl.AuditTimeliness_U @PipelineName VARCHAR(150),
									   @PipelineType VARCHAR(150),
									   @TimelyNumber INT,
									   @TimelyDatePart VARCHAR(20),
									   @MuteTimelyCheck BIT = 0
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON
	DECLARE @PipelineID INT,
			@PipelineTypeID TINYINT,
			@ErrorMessage VARCHAR(100)

	IF @PipelineName IS NOT NULL AND @PipelineType IS NOT NULL
	BEGIN
		SET @ErrorMessage = 'Only one input allowed at a time: @PipelineName or @PipelineType';
			THROW 51000, @ErrorMessage, 1;
	END
	IF (@TimelyNumber IS NULL OR @TimelyDatePart IS NULL) AND @MuteTimelyCheck=0
	BEGIN
		SET @ErrorMessage = '@TimelyNumber and @TimelyDartPart are both required to properly update if not muting';
		THROW 51000, @ErrorMessage, 1;
	END

	IF @PipelineName IS NOT NULL
	BEGIN
		SELECT @PipelineID = PipelineID
		FROM etl.Pipeline
		WHERE PipelineName = @PipelineName

		IF @PipelineID IS NULL
		BEGIN
			SET @ErrorMessage = 'Pipeline ''' + @PipelineName + ''' unable to be found';
			THROW 51000, @ErrorMessage, 1;
		END

		UPDATE etl.Pipeline
		SET TimelyNumber = CASE WHEN @TimelyNumber IS NULL THEN TimelyNumber ELSE @TimelyNumber END,
			TimelyDatePart = CASE WHEN @TimelyDatePart IS NULL THEN TimelyDatePart ELSE @TimelyDatePart END,
			MuteTimelyCheck = @MuteTimelyCheck
		WHERE PipelineID = @PipelineID
	END

	IF @PipelineType IS NOT NULL
	BEGIN
		SELECT @PipelineTypeID = PipelineTypeID
		FROM etl.PipelineType
		WHERE PipelineType = @PipelineType

		IF @PipelineTypeID IS NULL
		BEGIN
			SET @ErrorMessage = 'PipelineType ''' + @PipelineType + ''' unable to be found';
			THROW 51000, @ErrorMessage, 1;
		END

		UPDATE etl.PipelineType
		SET TimelyNumber = CASE WHEN @TimelyNumber IS NULL THEN TimelyNumber ELSE @TimelyNumber END,
			TimelyDatePart = CASE WHEN @TimelyDatePart IS NULL THEN TimelyDatePart ELSE @TimelyDatePart END,
			MuteTimelyCheck = @MuteTimelyCheck
		WHERE PipelineTypeID = @PipelineTypeID
	END

END
GO


DROP PROCEDURE IF EXISTS etl.AuditTimelinessPipelineCheck
GO
CREATE PROCEDURE etl.AuditTimelinessPipelineCheck
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DECLARE @Now DATETIMEOFFSET(2) = SYSDATETIMEOFFSET(),
			@MaxDML DATETIMEOFFSET(2),
			@DatePart VARCHAR(20),
			@Number INT,
			@CalculatedTime DATETIMEOFFSET(2)

	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
	DROP TABLE IF EXISTS #Pipelines
	;WITH CTE AS (
	SELECT
	p.PipelineID,
	pt.TimelyNumber,
	pt.TimelyDatePart
	FROM etl.PipelineType AS pt
	INNER JOIN etl.Pipeline AS p
		ON p.PipelineTypeID = pt.PipelineTypeID
	WHERE pt.MuteTimelyCheck = 0
	)
	SELECT
	p.PipelineID,
	p.PipelineName,
	p.LastTargetInsert,
	p.LastTargetUpdate,
	p.LastTargetDelete,
	p.TimelyNumber AS ChildTimelyNumber,
	p.TimelyDatePart AS ChildTimelyDatePart,
	pt.TimelyNumber AS ParentTimelyNumber,
	pt.TimelyDatePart AS ParentTimelyDatePart,
	0 AS Complete
	INTO #Pipelines
	FROM etl.Pipeline AS p
	INNER JOIN CTE AS pt
		ON pt.PipelineID = p.PipelineID
	WHERE p.MuteTimelyCheck = 0
	SET TRANSACTION ISOLATION LEVEL READ COMMITTED

	DROP TABLE IF EXISTS #FailResults
	CREATE TABLE #FailResults (
		PipelineID INT,
		PipelineName VARCHAR(150),
		LastDML DATETIMEOFFSET(2),
		TimelyNumber INT,
		TimelyDatePart VARCHAR(20)
	)

	DECLARE @PipelineID INT,
			@PipelineName VARCHAR(150),
			@LastTargetInsert DATETIMEOFFSET(2),
			@LastTargetUpdate DATETIMEOFFSET(2),
			@LastTargetDelete DATETIMEOFFSET(2),
			@ChildTimelyNumber INT,
			@ChildTimelyDatePart VARCHAR(20),
			@ParentTimelyNumber INT,
			@ParentTimelyDatePart VARCHAR(20)

	DECLARE timely_cursor CURSOR FOR
		SELECT
		PipelineID,
		PipelineName,
		LastTargetInsert,
		LastTargetUpdate,
		LastTargetDelete,
		ChildTimelyNumber,
		ChildTimelyDatePart,
		ParentTimelyNumber,
		ParentTimelyDatePart
		FROM #Pipelines
		WHERE Complete=0
		ORDER BY PipelineID

	OPEN timely_cursor
	FETCH NEXT FROM timely_cursor INTO
	@PipelineID, @PipelineName, @LastTargetInsert, @LastTargetUpdate, @LastTargetDelete, 
	@ChildTimelyNumber, @ChildTimelyDatePart, @ParentTimelyNumber, @ParentTimelyDatePart 

	WHILE @@FETCH_STATUS = 0
	BEGIN

		SELECT @MaxDML = MAX(TimelyValue) 
		FROM (VALUES(@LastTargetInsert), (@LastTargetUpdate), (@LastTargetDelete)) AS SUB(TimelyValue)

		SET @DatePart = ISNULL(@ChildTimelyDatePart, @ParentTimelyDatePart)
		SET @Number = ISNULL(@ChildTimelyNumber, @ParentTimelyNumber)

		SET @CalculatedTime = CASE WHEN @DatePart = 'MINUTE' THEN DATEADD(MINUTE, @Number, @MaxDML)
								   WHEN @DatePart = 'HOUR'   THEN DATEADD(HOUR, @Number, @MaxDML)
								   WHEN @DatePart = 'DAY'    THEN DATEADD(DAY, @Number, @MaxDML)
								   WHEN @DatePart = 'WEEK'   THEN DATEADD(WEEK, @Number, @MaxDML)
								   WHEN @DatePart = 'MONTH'  THEN DATEADD(MONTH, @Number, @MaxDML)
								   WHEN @DatePart = 'YEAR'   THEN DATEADD(YEAR, @Number, @MaxDML)
								   END

		IF @CalculatedTime < @Now
			INSERT INTO #FailResults (PipelineID, PipelineName, LastDML, TimelyNumber, TimelyDatePart)
			VALUES (@PipelineID, @PipelineName, @MaxDML, @Number, CASE WHEN @Number = 1 THEN LOWER(@DatePart) ELSE CONCAT(LOWER(@DatePart), 's') END)

		UPDATE #Pipelines
		SET Complete=1
		WHERE PipelineID = @PipelineID

		FETCH NEXT FROM timely_cursor INTO
		@PipelineID, @PipelineName, @LastTargetInsert, @LastTargetUpdate, @LastTargetDelete, 
		@ChildTimelyNumber, @ChildTimelyDatePart, @ParentTimelyNumber, @ParentTimelyDatePart 

	END
			
	CLOSE timely_cursor
	DEALLOCATE timely_cursor

	DROP TABLE #Pipelines

	IF EXISTS (SELECT 1/0 FROM #FailResults)
	BEGIN

		DECLARE @ErrorMessage VARCHAR(4000) = 'The following pipelines failed their timeliness checks: ' + CHAR(13) + ''
		SELECT @ErrorMessage += STRING_AGG('' + CAST(PipelineID AS VARCHAR(20)) + ': ''' + 
						  PipelineName + ''' has not had a DML operation within the timeframe: ' + CAST(TimelyNumber AS VARCHAR(10)) + ' ' + TimelyDatePart + 
						  '; Last DML Operation: ' + CAST(LastDML AS VARCHAR(50)) + ';', CHAR(13))
		FROM #FailResults
		
		DROP TABLE #FailResults;

		/* TODO: Configure Database Mail */

		/* Throw alert to stop pipeline */
		THROW 51000, @ErrorMessage, 1;
	END
END
GO


DROP PROCEDURE IF EXISTS etl.LogMaintenance_D
GO
CREATE PROCEDURE etl.LogMaintenance_D @RetentionDays INT,
									  @BatchSize INT = 10000,
									  @TotalAuditColumnLogRowsDeleted INT OUTPUT,
									  @TotalRunTimeRowsDeleted INT OUTPUT,
									  @TotalTimelinessRunTimeLogRowsDeleted INT OUTPUT,
									  @TotalAuditCustomLogRowsDeleted INT OUTPUT
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DECLARE @CurrentDate DATE = CAST(SYSDATETIMEOFFSET() AS DATE),
			@StartingDate DATETIME,
			@StartingDateTimeOffset DATETIMEOFFSET(2),
			@MaxAuditColumnLogID BIGINT,
			@MaxAuditCustomLogID BIGINT,
			@MaxRunTimeID BIGINT,
			@RowCount INT

	SET @TotalAuditColumnLogRowsDeleted = 0
	SET @TotalRunTimeRowsDeleted = 0
	SET @TotalTimelinessRunTimeLogRowsDeleted = 0
	SET @TotalAuditCustomLogRowsDeleted = 0

	SET @StartingDate = DATEADD(DAY, -@RetentionDays, @CurrentDate)
	/*SQL Server 2019 has CURRENT_TIMEZONE() that can be used here instead */
	SET @StartingDateTimeOffset = @StartingDate AT TIME ZONE 'Central Standard Time'
	PRINT @StartingDateTimeOffset

	SELECT @MaxRunTimeID = MAX(rt.RunTimeID)
	FROM  etl.RunTime AS rt
	WHERE rt.PipelineStart <= @StartingDate

	SELECT @MaxAuditColumnLogID = MAX(al.AuditColumnLogID)
	FROM etl.AuditColumnLog AS al
	WHERE RunTimeID <= @MaxRunTimeID

	SELECT @MaxAuditCustomLogID = MAX(al.AuditCustomLogID)
	FROM etl.AuditCustomLog AS al
	WHERE RunTimeID <= @MaxRunTimeID

	/* Delete out audit logs associated with run times past cutoff point */
	IF @MaxAuditColumnLogID IS NOT NULL
	BEGIN
		WHILE 1=1
		BEGIN
			DELETE TOP (@BatchSize)
			FROM etl.AuditColumnLog
			WHERE AuditColumnLogID <= @MaxAuditColumnLogID

			SET @RowCount = @@ROWCOUNT
			SET @TotalAuditColumnLogRowsDeleted += @RowCount

			IF @RowCount < @BatchSize
				BREAK
		END
	END

	/* Delete out run times timeliness logs past cutoff point */
	IF @MaxRunTimeID IS NOT NULL
	BEGIN
		WHILE 1=1
		BEGIN
			DELETE TOP (@BatchSize)
			FROM etl.TimelinessRunTimeLog
			WHERE RunTimeID <= @MaxRunTimeID

			SET @RowCount = @@ROWCOUNT
			SET @TotalTimelinessRunTimeLogRowsDeleted += @RowCount

			IF @RowCount < @BatchSize
				BREAK
		END
	END

	IF @MaxAuditCustomLogID IS NOT NULL
	BEGIN
		WHILE 1=1
		BEGIN
			DELETE TOP (@BatchSize)
			FROM etl.AuditCustomLog
			WHERE RunTimeID <= @MaxRunTimeID

			SET @RowCount = @@ROWCOUNT
			SET @TotalAuditCustomLogRowsDeleted += @RowCount

			IF @RowCount < @BatchSize
				BREAK
		END
	END

	/* Delete out run times past cutoff point */
	IF @MaxRunTimeID IS NOT NULL
	BEGIN
		WHILE 1=1
		BEGIN
			DELETE TOP (@BatchSize)
			FROM etl.RunTime
			WHERE RunTimeID <= @MaxRunTimeID

			SET @RowCount = @@ROWCOUNT
			SET @TotalRunTimeRowsDeleted += @RowCount

			IF @RowCount < @BatchSize
				BREAK
		END
	END
END
GO


DROP PROCEDURE IF EXISTS etl.Pipeline_UI
GO
CREATE PROCEDURE [etl].[Pipeline_UI] @PipelineName VARCHAR(150),
								   @PipelineType VARCHAR(150),
								   @PipelineArgs VARCHAR(4000) = NULL,
								   @SourceFullAddress VARCHAR(150) = NULL,
								   @SourceAddressType VARCHAR(150) = NULL,
								   @TargetFullAddress VARCHAR(150) = NULL,
								   @TargetAddressType VARCHAR(150) = NULL,
								   @PipelineID INT OUTPUT,
								   @NextWaterMark VARCHAR(50) = NULL,
								   @WaterMark VARCHAR(50) = NULL OUTPUT,
								   @LoadLineage BIT = 0 OUTPUT,
								   @Active BIT = 1 OUTPUT
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	SELECT @PipelineID = PipelineID,
		   @LoadLineage = LoadLineage,
		   @Active = Active
	FROM etl.Pipeline
	WHERE PipelineName = @PipelineName

	IF @PipelineID IS NOT NULL
	BEGIN
		IF @NextWaterMark IS NOT NULL
		BEGIN
			DECLARE @WaterMarkTable TABLE (WaterMark VARCHAR(50))

			/* Update next watermark to last value seen */
			UPDATE etl.Pipeline
			SET NextWaterMark = @NextWaterMark
			OUTPUT deleted.WaterMark /* Grab current WaterMark */
			INTO @WaterMarkTable
			WHERE PipelineID = @PipelineID

			SELECT @WaterMark = WaterMark
			FROM @WaterMarkTable
		END
	END
	ELSE /* Create Pipeline */
	BEGIN
		SET @Active = 1

		DECLARE @PipelineTypeID INT,
				@SourceAddressID INT,
				@TargetAddressID INT

		/* Cannot depend on specific ID, so seeking from text */
		SELECT @PipelineTypeID = PipelineTypeID 
		FROM etl.PipelineType 
		WHERE PipelineType = @PipelineType

		IF @PipelineTypeID IS NULL
		BEGIN
			DECLARE @ErrorMessage VARCHAR(100) = 'Unable to find PipelineType: ' + @PipelineType + '';
			THROW 51000, @ErrorMessage, 1;
		END

		/* Create addresses if needed */
		IF @SourceFullAddress IS NOT NULL
			EXEC etl.Address_SI @FullAddress = @SourceFullAddress,
								@AddressType = @SourceAddressType,
								@AddressID = @SourceAddressID OUTPUT
		IF @TargetFullAddress IS NOT NULL
			EXEC etl.Address_SI @FullAddress = @TargetFullAddress,
								@AddressType = @TargetAddressType,
								@AddressID = @TargetAddressID OUTPUT

		INSERT INTO etl.Pipeline (
			PipelineName, 
			PipelineTypeID,
			PipelineArgs,
			WaterMark,
			NextWaterMark, 
			SourceAddressID, 
			TargetAddressID)
		VALUES (
			@PipelineName, 
			@PipelineTypeID, 
			@PipelineArgs,
			@WaterMark,
			@NextWaterMark,
			@SourceAddressID,
			@TargetAddressID)

		SET @PipelineID = SCOPE_IDENTITY()
	END

END
GO


DROP PROCEDURE IF EXISTS etl.RunTime_I
GO
CREATE PROCEDURE etl.RunTime_I @PipelineName VARCHAR(150),
									   @PipelineType VARCHAR(150),
									   @FullLoad BIT = NULL,
									   @SourceFullAddress VARCHAR(150) = NULL,
									   @SourceAddressType VARCHAR(150) = NULL,
									   @TargetFullAddress VARCHAR(150) = NULL,
									   @TargetAddressType VARCHAR(150) = NULL,
									   @NextWaterMark VARCHAR(50) = NULL,
									   @ParentRunTimeID BIGINT = NULL,
									   @WaterMark VARCHAR(50) = NULL OUTPUT,
									   @RunTimeID BIGINT OUTPUT,
									   @PipelineID INT = NULL OUTPUT,
									   @LoadLineage BIT = 0 OUTPUT
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON
	DECLARE @Active BIT,
			@ErrorMessage VARCHAR(100)

	/* Create pipeline if needed, increment watermark if needed. Grab PipelineID. */
	EXEC etl.Pipeline_UI @PipelineName = @PipelineName,
					    @PipelineType = @PipelineType,
						@SourceFullAddress = @SourceFullAddress,
						@SourceAddressType = @SourceAddressType,
						@TargetFullAddress = @TargetFullAddress,
						@TargetAddressType = @TargetAddressType,
						@NextWaterMark = @NextWaterMark,
						@WaterMark = @WaterMark OUTPUT,
					    @PipelineID = @PipelineID OUTPUT,
						@LoadLineage = @LoadLineage OUTPUT,
						@Active = @Active OUTPUT

	IF @Active = 0
	BEGIN
		SET @ErrorMessage = 'Pipeline ''' + @PipelineName + ''' is marked inactive, execution must be bypassed in the pipeline if @Active = 0.' + CHAR(13) + 
							'You can achieve this using the etl.PipelineActive_S stored procedure to check the active flag before execution.';
		THROW 51000, @ErrorMessage, 1;
	END

	/* Create run time */
	INSERT INTO etl.RunTime (
		ParentRunTimeID, 
		PipelineID, 
		PipelineStart,
		FullLoad,
		WaterMark,
		NextWaterMark)
	VALUES (
		@ParentRunTimeID, 
		@PipelineID,
		SYSDATETIMEOFFSET(),
		@FullLoad,
		@WaterMark,
		@NextWaterMark)

	SET @RunTimeID = SCOPE_IDENTITY()

END
GO


DROP PROCEDURE IF EXISTS etl.AuditLoad
GO
CREATE PROCEDURE etl.AuditLoad @ParentRunTimeID BIGINT = NULL,
								@RunTimeID BIGINT,
								@PipelineID INT,
								@FullLoad BIT = 1,
								@Debug BIT = 0,
								@AuditType VARCHAR(150)
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON
	DECLARE @ErrorMessage VARCHAR(100)

	IF @FullLoad = 0 
		AND @ParentRunTimeID IS NULL 
		AND (@AuditType = 'Completeness' OR @AuditType = 'Accuracy')
	BEGIN
		SET @ErrorMessage = '@ParentRunTimeID is required for incremental (@FullLoad=0) Completeness/Accuracy audits';
		THROW 51000, @ErrorMessage, 1;
	END

	/* Grab pipeline args to determine if chunking should occur, only allowed for full loads */
	DECLARE @ChunkOut BIT
	SELECT @ChunkOut = ISNULL(JSON_VALUE(PipelineArgs, '$.info.audit.chunkout'), 1)
	FROM etl.Pipeline
	WHERE PipelineID = @PipelineID

	IF @AuditType = 'Completeness'
	BEGIN
		IF @ChunkOut=1
			BEGIN /* Recursive check that chunks out dataset and logs results */
				EXEC etl.AuditCompletenessBatch_I @ParentRunTimeID = @ParentRunTimeID,
												  @RunTimeID = @RunTimeID,
												  @PipelineID = @PipelineID,
												  @FullLoad = @FullLoad,
												  @Debug = @Debug
			END
		ELSE
			BEGIN
				/* Scans once and logs results */
				EXEC etl.AuditCompleteness_I @ParentRunTimeID = @ParentRunTimeID,
											 @RunTimeID = @RunTimeID,
											 @PipelineID = @PipelineID,
											 @FullLoad = @FullLoad,
											 @Debug = @Debug
			END
	END

	IF @AuditType = 'Accuracy'
	BEGIN
		IF @ChunkOut=1
			BEGIN /* Recursive check that chunks out dataset and logs results */
				EXEC etl.AuditAccuracyBatch_I @ParentRunTimeID = @ParentRunTimeID,
											  @RunTimeID = @RunTimeID,
											  @PipelineID = @PipelineID,
											  @FullLoad = @FullLoad,
											  @Debug = @Debug
			END
		ELSE
			BEGIN
				/* Scans once and logs results */
				EXEC etl.AuditAccuracy_I @ParentRunTimeID = @ParentRunTimeID,
										 @RunTimeID = @RunTimeID,
										 @PipelineID = @PipelineID,
										 @FullLoad = @FullLoad,
										 @Debug = @Debug
			END
	END

	IF (@AuditType = 'Accuracy' OR @AuditType = 'Completeness')
	BEGIN
		/* Check alerts against logged data */
		EXEC etl.AuditColumnLogCheck @RunTimeID = @RunTimeID,
									 @PipelineID = @PipelineID,
									 @AuditType = @AuditType
	END

	IF @AuditType = 'Timeliness'
	BEGIN
		/* Execute timeliness check against pipelines */
		EXEC etl.AuditTimelinessRunTime_I @PipelineID = @PipelineID
		EXEC etl.AuditTimelinessPipelineCheck

	END

	IF @AuditType = 'Custom'
	BEGIN
		EXEC etl.AuditCustomCheck @ParentRunTimeID = @ParentRunTimeID,
								  @RunTimeID = @RunTimeID,
								  @PipelineID = @PipelineID,
								  @Debug = @Debug
	END

END
GO

DROP PROCEDURE IF EXISTS etl.RunTime_U
GO
CREATE PROCEDURE etl.RunTime_U @RunTimeID BIGINT,
										@Inserts INT = NULL,
										@Updates INT = NULL,
										@SoftDeletes INT = NULL,
										@TotalRows INT = NULL
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DECLARE @PipelineEnd DATETIMEOFFSET(2) = SYSDATETIMEOFFSET()
	DECLARE @PipelineTable TABLE (PipelineID INT)

	/* Complete run time with DML metadata */
	UPDATE etl.RunTime
	SET PipelineEnd = @PipelineEnd,
		RunTimeSeconds = DATEDIFF(SECOND, PipelineStart, @PipelineEnd),
		Completed = 1,
		Inserts = @Inserts,
		Updates = @Updates,
		SoftDeletes = @SoftDeletes,
		TotalRows = @TotalRows
	OUTPUT deleted.PipelineID INTO @PipelineTable
	WHERE RunTimeID = @RunTimeID

	/* Update pipeline timeliness for DML operations */
	UPDATE etl.Pipeline
	SET WaterMark = NextWaterMark,
		LastTargetInsert = CASE WHEN @Inserts > 0 THEN @PipelineEnd ELSE LastTargetInsert END,
		LastTargetUpdate = CASE WHEN @Updates > 0 THEN @PipelineEnd ELSE LastTargetUpdate END,
		LastTargetDelete = CASE WHEN @SoftDeletes > 0 THEN @PipelineEnd ELSE LastTargetDelete END,
		LoadLineage = 0
	WHERE PipelineID = (SELECT PipelineID FROM @PipelineTable)

END
GO


DROP PROCEDURE IF EXISTS etl.PipelineUser_SI
GO
CREATE PROCEDURE etl.PipelineUser_SI @UserName VARCHAR(150),
									@PipelineUserID INT OUTPUT
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	/* Grab PipelineUserID */
	SELECT @PipelineUserID = PipelineUserID
	FROM etl.PipelineUser
	WHERE UserName = @UserName

	/* If cannot be found, create one */
	IF @PipelineUserID IS NULL
	BEGIN
		INSERT INTO etl.PipelineUser (UserName)
		VALUES (@UserName)

		SET @PipelineUserID = SCOPE_IDENTITY()
	END

END
GO


DROP PROCEDURE IF EXISTS etl.AuditTimelinessRunTime_I
GO
CREATE PROCEDURE etl.AuditTimelinessRunTime_I @PipelineID INT
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DECLARE @SecondsThreshold INT,
			@NextWaterMark BIGINT,
			@WaterMark BIGINT,
			@PipelineArgsVar VARCHAR(4000),
			@Timestamp DATETIMEOFFSET(2) = SYSDATETIMEOFFSET()

	DECLARE @PipelineArgs TABLE (
		WaterMark BIGINT,
		PipelineArgs VARCHAR(4000)
	)

	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
	SELECT @NextWaterMark = MAX(RunTimeID) 
	FROM etl.RunTime 
	WHERE PipelineEnd <= @Timestamp
	SET TRANSACTION ISOLATION LEVEL READ COMMITTED

	UPDATE etl.Pipeline
	SET NextWaterMark = @NextWaterMark
	OUTPUT deleted.WaterMark, deleted.PipelineArgs
	INTO @PipelineArgs
	WHERE PipelineID = @PipelineID

	SET @WaterMark = (SELECT ISNULL(WaterMark, 0) FROM @PipelineArgs)
	SET @PipelineArgsVar = (SELECT PipelineArgs FROM @PipelineArgs)

	/* Default alert threshold, any run time over 30 minutes */
	SET @SecondsThreshold =ISNULL(JSON_VALUE(@PipelineArgsVar, '$.info.audit.timeliness_run_time_check_seconds'), 1800)

	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
	DROP TABLE IF EXISTS #Results
	;WITH CTE AS (
	SELECT
	RunTimeID
	FROM etl.RunTime
	WHERE RunTimeID > @WaterMark
		AND RunTimeID <= @NextWaterMark
	)
	SELECT
	rt.RunTimeID,
	rt.PipelineID,
	rt.RunTimeSeconds
	INTO #Results
	FROM etl.RunTime AS rt
	INNER JOIN CTE
		ON CTE.RunTimeID = rt.RunTimeID
	WHERE rt.RunTimeSeconds > @SecondsThreshold
	SET TRANSACTION ISOLATION LEVEL READ COMMITTED

	INSERT INTO etl.TimelinessRunTimeLog (RunTimeID, PipelineID, RunTimeSeconds, SecondsThreshold)
	SELECT
	RunTimeID,
	PipelineID,
	RunTimeSeconds,
	@SecondsThreshold
	FROM #Results AS r
	WHERE NOT EXISTS (
		SELECT 1/0
		FROM etl.TimelinessRunTimeLog AS t
		WHERE t.RunTimeID = r.RunTimeID
	)

END
GO


DROP PROCEDURE IF EXISTS etl.Pipeline_D
GO
CREATE PROCEDURE etl.Pipeline_D @PipelineName VARCHAR(150),
								@BatchSize INT = 10000
AS
BEGIN
SET NOCOUNT ON 
SET XACT_ABORT ON	
	DECLARE @PipelineID INT,
			@TargetAddressID INT,
			@ErrorMessage VARCHAR(100),
			@Message VARCHAR(100),
			@RowCount INT

	SELECT 
	@PipelineID = PipelineID,
	@TargetAddressID = TargetAddressID
	FROM etl.Pipeline
	WHERE PipelineName = @PipelineName

	IF @PipelineID IS NULL
	BEGIN
		SET @ErrorMessage = 'Pipeline ''' + @PipelineName + ''' is unable to be found';
		THROW 51000, @ErrorMessage, 1;
	END

	WHILE 1=1
	BEGIN
		DELETE TOP (@BatchSize) t
		FROM etl.TimelinessRunTimeLog AS t
		INNER JOIN etl.RunTime AS rt
			ON rt.RunTimeID = t.RunTimeID
		WHERE rt.PipelineID = @PipelineID

		SET @RowCount = @@ROWCOUNT
		IF @RowCount < @BatchSize
			BREAK
	END

	WHILE 1=1
	BEGIN
		DELETE TOP (@BatchSize) t
		FROM etl.AuditColumnLog AS t
		INNER JOIN etl.AuditColumnRule AS ar
			ON ar.AuditColumnRuleID = t.AuditColumnRuleID
		WHERE ar.PipelineID = @PipelineID

		SET @RowCount = @@ROWCOUNT
		IF @RowCount < @BatchSize
			BREAK
	END

	WHILE 1=1
	BEGIN
		DELETE TOP (@BatchSize) t
		FROM etl.AuditColumnRule AS t
		WHERE t.PipelineID = @PipelineID

		SET @RowCount = @@ROWCOUNT
		IF @RowCount < @BatchSize
			BREAK
	END

	WHILE 1=1
	BEGIN
		DELETE TOP (@BatchSize) t
		FROM etl.AuditCustomLog AS t
		INNER JOIN etl.AuditCustomRule AS acr
			ON acr.AuditCustomRuleID = t.AuditCustomRuleID
		WHERE acr.PipelineID = @PipelineID

		SET @RowCount = @@ROWCOUNT
		IF @RowCount < @BatchSize
			BREAK
	END

	WHILE 1=1
	BEGIN
		DELETE TOP (@BatchSize) t
		FROM etl.AuditCustomRule AS t
		WHERE t.PipelineID = @PipelineID

		SET @RowCount = @@ROWCOUNT
		IF @RowCount < @BatchSize
			BREAK
	END

	WHILE 1=1
	BEGIN
		DELETE TOP (@BatchSize) t
		FROM etl.RunTime AS t
		WHERE PipelineID = @PipelineID

		SET @RowCount = @@ROWCOUNT
		IF @RowCount < @BatchSize
			BREAK
	END

	DELETE t
	FROM etl.Pipeline AS t
	WHERE PipelineID = @PipelineID

END
GO


DROP PROCEDURE IF EXISTS etl.DeprecationDeclared_UID
GO
CREATE PROCEDURE etl.DeprecationDeclared_UID @FullAddress VARCHAR(150),
											@Revert BIT = 0,
											@RenameTable BIT = 1,
											@Debug BIT = 0
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON
	DECLARE @ErrorMessage VARCHAR(100),
			@SQLQuery NVARCHAR(MAX),
			@AddressTypeID INT,
			@AddressType VARCHAR(150),
			@AddressID INT,
			@DatabaseName VARCHAR(50),
			@SchemaName VARCHAR(50),
			@TableName VARCHAR(50),
			@Message VARCHAR(150)

	DECLARE @CatchErrorMessage NVARCHAR(MAX),
			@ErrorSeverity INT,
			@ErrorState INT

	;WITH CTE AS (
		SELECT
		AddressID
		FROM etl.Address
		WHERE FullAddress = @FullAddress
	)
	SELECT
	@AddressID = a.AddressID,
	@AddressTypeID = a.AddressTypeID,
	@DatabaseName = a.DatabaseName,
	@SchemaName = a.SchemaName,
	@TableName = a.TableName
	FROM etl.Address AS a
	INNER JOIN CTE
		ON CTE.AddressID = a.AddressID

	SELECT
	@AddressType = AddressType
	FROM etl.AddressType
	WHERE AddressTypeID = @AddressTypeID

	IF @AddressID IS NULL
	BEGIN
		SET @ErrorMessage = 'Address: ''' + @FullAddress + ''' unable to be found';
		THROW 51000, @ErrorMessage, 1;
	END

	/* Grab pipelines associated with address to de-activate */
	DROP TABLE IF EXISTS #Pipelines
	CREATE TABLE #Pipelines (PipelineID INT)
	INSERT INTO #Pipelines (PipelineID)
	SELECT
	PipelineID
	FROM etl.Pipeline AS p
	INNER JOIN etl.Address AS s
		ON s.AddressID = p.SourceAddressID
	UNION 
	SELECT
	PipelineID
	FROM etl.Pipeline AS p
	INNER JOIN etl.Address AS t
		ON t.AddressID = p.TargetAddressID

	/* Grab first layer of dependencies for address */
	DROP TABLE IF EXISTS #AddressLineageAffected
	CREATE TABLE #AddressLineageAffected ( AddressID INT )
	INSERT INTO #AddressLineageAffected (AddressID)
	SELECT
	TargetAddressID AS AddressID
	FROM etl.AddressDependency AS ad
	WHERE TargetAddressID = @AddressID
	UNION
	SELECT
	SourceAddressID AS AddressID
	FROM etl.AddressDependency AS ad
	WHERE SourceAddressID = @AddressID

	/* Grab all pipelines associated with address dependencies */
	DROP TABLE IF EXISTS #PipelineLineageAffected
	CREATE TABLE #PipelineLineageAffected (PipelineID INT)
	INSERT INTO #PipelineLineageAffected (PipelineID)
	SELECT
	PipelineID
	FROM etl.Pipeline AS p
	INNER JOIN #AddressLineageAffected AS s
		ON s.AddressID = p.SourceAddressID
	UNION
	SELECT
	PipelineID
	FROM etl.Pipeline AS p
	INNER JOIN #AddressLineageAffected AS t
		ON t.AddressID = p.TargetAddressID

	/* Set all pipelines to reload */
	/* We'll delete the target lineage */
	/* When physical objects are removed the lineage will be checked again to ensure its accurate */
	/* Can't remove the data in the future if the address is found in the dependency table */
	UPDATE t
	SET LoadLineage = 1
	FROM etl.Pipeline AS t
	INNER JOIN #PipelineLineageAffected AS p
		ON p.PipelineID = t.PipelineID

	DROP TABLE #AddressLineageAffected
	DROP TABLE #PipelineLineageAffected

	DECLARE @DeprecationTable TABLE (
		AddressID INT,
		ID INT,
		IDType VARCHAR(150),
		IDName VARCHAR(150)
	)

	IF @Revert = 0
	BEGIN
		BEGIN TRY
			BEGIN TRANSACTION

			/* Delete any dependencies */
			/* We flagged lineage reload to check that lineage records are accurate */
			DELETE FROM etl.AddressDependency
			WHERE TargetAddressID = @AddressID

			/* Update associated pipelines to be inactive */
			UPDATE t
			SET Active = 0,
				MuteTimelyCheck = 0
			OUTPUT @AddressID, deleted.PipelineID, 'PipelineID', deleted.PipelineName
			INTO @DeprecationTable
			FROM etl.Pipeline AS t
			INNER JOIN #Pipelines AS p
				ON p.PipelineID = t.PipelineID

			/* Update address to have deprecated flag */
			UPDATE etl.Address
			SET Deprecated = 1
			WHERE AddressID = @AddressID

			/* Update audit rules to be inactive */
			UPDATE t
			SET RuleEndDate = CAST(SYSDATETIMEOFFSET() AS DATE),
				Active = 0
			OUTPUT @AddressID, deleted.AuditColumnRuleID, 'AuditColumnRuleID', deleted.AuditColumn
			INTO @DeprecationTable
			FROM etl.AuditColumnRule AS t
			INNER JOIN #Pipelines AS p
				ON p.PipelineID = t.PipelineID
			WHERE t.Active=1

			UPDATE t
			SET RuleEndDate = CAST(SYSDATETIMEOFFSET() AS DATE),
				Active = 0
			OUTPUT @AddressID, deleted.AuditCustomRuleID, 'AuditCustomRuleID', NULL
			INTO @DeprecationTable
			FROM etl.AuditCustomRule AS t
			INNER JOIN #Pipelines AS p
				ON p.PipelineID = t.PipelineID
			WHERE t.Active=1

			/* Rename TableName to TableName_Deprecated if flagged */
			IF @AddressType = 'Database' AND DB_NAME() = @DatabaseName AND @RenameTable = 1
			BEGIN
				SET @SQLQuery = 'EXEC sp_rename ''' + @SchemaName + '.' + @TableName + ''', ''' + @TableName + '_Deprecated' + ''';'
				IF @Debug = 1
				BEGIN
					PRINT @SQLQuery
				END
				ELSE
				BEGIN
					EXEC sys.sp_executesql @SQLQuery
				END
				SET @Message = 'Table: ' + @DatabaseName + '.' + @SchemaName + '.' + @TableName + ' has been renamed to: ' + @DatabaseName + '.' + @SchemaName + '.' + @TableName + '_Deprecated'
				RAISERROR(@Message,0,1) WITH NOWAIT;
			END

			/* Insert the records into the declared deprecation table */
			INSERT INTO etl.DeprecationDeclared (AddressID, ID, IDType, IDName, MarkedForDeprecation)
			SELECT
			dt.AddressID,
			dt.ID,
			dt.IDType,
			dt.IDName,
			SYSDATETIMEOFFSET()
			FROM @DeprecationTable AS dt
			WHERE NOT EXISTS (
				SELECT 1/0
				FROM etl.DeprecationDeclared AS d
				WHERE d.AddressID = dt.AddressID
					AND d.ID = dt.ID
					AND d.IDType = dt.IDType
				)
			COMMIT TRANSACTION
		END TRY
		BEGIN CATCH
			SET @CatchErrorMessage = ERROR_MESSAGE() + N' Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(30)) + 
								N' Line: ' + CAST(ERROR_LINE() AS NVARCHAR(30)) + 
								N' Procedure: ' + ERROR_PROCEDURE()
			SET @ErrorSeverity = ERROR_SEVERITY()
			SET @ErrorState = ERROR_STATE()

			ROLLBACK;

			RAISERROR(@CatchErrorMessage, @ErrorSeverity, @ErrorState)

		END CATCH
	END
	ELSE
	BEGIN
		BEGIN TRY
			BEGIN TRANSACTION
			/* Activate the records again and load the lineage again */
			UPDATE t
			SET Active = 1,
				MuteTimelyCheck = 0,
				LoadLineage = 1
			OUTPUT @AddressID, deleted.PipelineID, 'PipelineID', deleted.PipelineName
			INTO @DeprecationTable
			FROM etl.Pipeline AS t
			INNER JOIN #Pipelines AS p
				ON p.PipelineID = t.PipelineID

			/* "un-deprecate" address record */
			UPDATE etl.Address
			SET Deprecated = 0
			WHERE AddressID = @AddressID
		
			/* Re-activate audit rules */
			UPDATE t
			SET RuleEndDate = '9999-12-31',
				Active = 1
			OUTPUT @AddressID, deleted.AuditColumnRuleID, 'AuditColumnRuleID', deleted.AuditColumn
			INTO @DeprecationTable
			FROM etl.AuditColumnRule AS t
			INNER JOIN etl.DeprecationDeclared AS d
				ON d.ID = t.AuditColumnRuleID
				AND d.IDType = 'AuditColumnRuleID'
				AND d.AddressID = @AddressID

			UPDATE t
			SET RuleEndDate = '9999-12-31',
				Active = 1
			OUTPUT @AddressID, deleted.AuditCustomRuleID, 'AuditCustomRuleID', NULL
			INTO @DeprecationTable
			FROM etl.AuditCustomRule AS t
			INNER JOIN etl.DeprecationDeclared AS d
				ON d.ID = t.AuditCustomRuleID
				AND d.IDType = 'AuditCustomRuleID'
				AND d.AddressID = @AddressID

			/* Rename from TableName_Deprecated back to TableName if flagged */
			IF @AddressType = 'Database' AND DB_NAME() = @DatabaseName AND @RenameTable = 1
			BEGIN
				SET @SQLQuery = 'EXEC sp_rename ''' + @SchemaName + '.' + @TableName + '_Deprecated' + ''', ''' + @TableName + ''';'
				IF @Debug = 1
				BEGIN
					PRINT @SQLQuery
				END
				ELSE
				BEGIN
					EXEC sys.sp_executesql @SQLQuery
				END
				SET @Message = 'Table: ' + @DatabaseName + '.' + @SchemaName + '.' + @TableName + '_Deprecated has been renamed back to: ' + @DatabaseName + '.' + @SchemaName + '.' + @TableName + ''
				RAISERROR(@Message,0,1) WITH NOWAIT;
			END

			/* Remove the IDs from the deprecation table since no longer declared */
			DELETE d 
			FROM etl.DeprecationDeclared AS d
			INNER JOIN @DeprecationTable AS dt
				ON dt.AddressID = d.AddressID
				AND dt.ID = d.ID
				AND dt.IDType = d.IDType
			COMMIT TRANSACTION
		END TRY 
		BEGIN CATCH
			SET @CatchErrorMessage = ERROR_MESSAGE() + N' Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(30)) + 
								N' Line: ' + CAST(ERROR_LINE() AS NVARCHAR(30)) + 
								N' Procedure: ' + ERROR_PROCEDURE()
			SET @ErrorSeverity = ERROR_SEVERITY()
			SET @ErrorState = ERROR_STATE()

			ROLLBACK;

			RAISERROR(@CatchErrorMessage, @ErrorSeverity, @ErrorState)

		END CATCH
	END

	IF @@TRANCOUNT > 0 
		ROLLBACK;

	DROP TABLE #Pipelines

	SELECT
	'etl.DeprecationDeclared' AS TableName,
	*
	FROM etl.DeprecationDeclared
	WHERE AddressID = @AddressID

END
GO


DROP PROCEDURE IF EXISTS etl.DeprecationFinalCheck
GO
CREATE PROCEDURE etl.DeprecationFinalCheck @WaitingDays INT
AS
BEGIN
SET NOCOUNT ON 
SET XACT_ABORT ON

	DECLARE @Now DATETIMEOFFSET(2) = SYSDATETIMEOFFSET(),
			@Message VARCHAR(1000)

	IF @WaitingDays < 0
	BEGIN
		SET @Message = '@WaitingDays variable must be a positive value';
		THROW 51000, @Message, 1;
	END

	/* If any deprecation records past waiting, throw alert */
	IF EXISTS (
		SELECT 1/0
		FROM etl.DeprecationDeclared
		WHERE DATEADD(DAY, @WaitingDays, MarkedForDeprecation) < @Now
			AND TicketsCreated = 0
	)
	BEGIN
		DROP TABLE IF EXISTS #Addresses
		;WITH CTE AS (
		SELECT
		DeprecationDeclaredID
		FROM etl.DeprecationDeclared
		WHERE DATEADD(DAY, @WaitingDays, MarkedForDeprecation) < @Now
			AND TicketsCreated = 0
		)
		SELECT DISTINCT
		FullAddress
		INTO #Addresses
		FROM etl.DeprecationDeclared AS d
		INNER JOIN CTE
			ON CTE.DeprecationDeclaredID = d.DeprecationDeclaredID
		INNER JOIN etl.Address AS a
			ON a.AddressID = d.AddressID;

		/* semantics */
		DECLARE @Day VARCHAR(10)
		IF @WaitingDays = 1
		BEGIN SET @Day = 'day' END
		ELSE BEGIN SET @Day = 'days' END

		SELECT
		@Message = 'The following addresses are ready for final deprecation after waiting ' + CAST(@WaitingDays AS VARCHAR(20)) + ' ' + @Day + ':' + CHAR(13) + STRING_AGG(FullAddress,', ')
		FROM #Addresses;

		/* TODO: Configure Database Mail */

		PRINT @Message
	END
END
GO


DROP PROCEDURE IF EXISTS etl.DeprecationFinal_D
GO
CREATE PROCEDURE etl.DeprecationFinal_D @FullAddress VARCHAR(150)
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON
	DECLARE @ErrorMessage VARCHAR(100),
			@AddressID INT

	SELECT @AddressID = AddressID
	FROM etl.Address
	WHERE FullAddress = @FullAddress

	IF @AddressID IS NULL
	BEGIN
		SET @ErrorMessage = 'Address ''' + @FullAddress + ''' cannot be found.';
		THROW 51000, @ErrorMessage, 1;
	END

	IF EXISTS (
		SELECT 1/0
		FROM etl.AddressDependency
		WHERE SourceAddressID = @AddressID
	) OR EXISTS (
		SELECT 1/0
		FROM etl.AddressDependency
		WHERE TargetAddressID = @AddressID
	)
	BEGIN
		SET @ErrorMessage = 'Address was found in etl.AddressDependency, fix address lineage before proceeding';
		THROW 51000, @ErrorMessage, 1;
	END

	DROP TABLE IF EXISTS #PipelineDeprecation
	SELECT
	p.PipelineID,
	p.PipelineName,
	0 AS Complete
	INTO #PipelineDeprecation
	FROM etl.DeprecationDeclared AS d
	INNER JOIN etl.Pipeline AS p
		ON p.PipelineID = d.ID
	WHERE d.AddressID = @AddressID
		AND d.IDType = 'PipelineID'

	IF NOT EXISTS (
		SELECT 1/0
		FROM #PipelineDeprecation
	)
	BEGIN
		SET @ErrorMessage = 'No pipelines in etl.DeprecationDeclared to be deprecated, no need to run';
		THROW 51000, @ErrorMessage, 1;
	END

	/* Proceed to delete pipelines associated with address marked for deprecation */
	DECLARE @PipelineName VARCHAR(150)

	DECLARE deprecation_cursor CURSOR FOR
		SELECT
		PipelineName
		FROM #PipelineDeprecation
		WHERE Complete=0
		ORDER BY PipelineID

	OPEN deprecation_cursor
	FETCH NEXT FROM deprecation_cursor INTO @PipelineName

	WHILE @@FETCH_STATUS = 0
	BEGIN

		EXEC etl.Pipeline_D @PipelineName = @PipelineName

		UPDATE #PipelineDeprecation
		SET Complete=1
		WHERE PipelineName = @PipelineName

		FETCH NEXT FROM deprecation_cursor INTO @PipelineName

	END
	CLOSE deprecation_cursor
	DEALLOCATE deprecation_cursor

	DROP TABLE #PipelineDeprecation

	/* Update any remaining pipeline source address id to be null instead */
	UPDATE t
	SET SourceAddressID = NULL
	FROM etl.Pipeline AS t
	WHERE SourceAddressID = @AddressID

	/* After all pipeline records and logs are deleted, delete address */
	DELETE FROM etl.Address
	WHERE AddressID = @AddressID
		AND Deprecated=1

	/* Remove marked deprecation records, revert not possible anymore */
	DELETE FROM etl.DeprecationDeclared
	WHERE AddressID = @AddressID

	PRINT 'All data associated with Address ''' + @FullAddress + ''' has been removed.'

END
GO


DROP PROCEDURE IF EXISTS etl.PipelineActive_S
GO
CREATE PROCEDURE etl.PipelineActive_S @PipelineName VARCHAR(150),
									 @Active BIT OUTPUT
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	SELECT
	@Active = Active
	FROM etl.Pipeline
	WHERE PipelineName = @PipelineName

	/* If the pipeline can't be found, assume not created yet and will be created */
	IF @Active IS NULL
		SET @Active = 1

END
GO


DROP PROCEDURE IF EXISTS etl.AuditCustomRule_I
GO
CREATE PROCEDURE etl.AuditCustomRule_I @PipelineName VARCHAR(150),
										@CustomSQL NVARCHAR(MAX),
										@CustomAlertMessage VARCHAR(400),
										@InfoAlert BIT = NULL,
										@ErrorAlert BIT = NULL
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON
	DECLARE @ErrorMessage VARCHAR(200),
			@PipelineID INT

	/* Check inputs */
	IF (@InfoAlert IS NULL OR @ErrorAlert IS NULL) OR (@InfoAlert=1 AND @ErrorAlert=1) OR (@InfoAlert=0 AND @ErrorAlert=0)
	BEGIN
		SET @ErrorMessage = 'Must declare @InfoAlert and @ErrorAlert variables, one must be active';
		THROW 51000, @ErrorMessage, 1;
	END
	IF (@CustomSQL IS NULL OR @CustomAlertMessage IS NULL)
	BEGIN
		SET @ErrorMessage = '@CustomSQL and @CustomAlertMessage are both required';
		THROW 51000, @ErrorMessage, 1;
	END

	/* Begin script */
	DECLARE @RuleStartDate DATE = CAST(SYSDATETIMEOFFSET() AS DATE),
			@RuleEndDate DATE = '9999-12-31',
			@QueryHash VARBINARY(32) = CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', @CustomSQL))

	SELECT @PipelineID = PipelineID
	FROM etl.Pipeline
	WHERE PipelineName = @PipelineName

	DROP TABLE IF EXISTS #Queries
	SELECT
	CONVERT(VARBINARY(32), HASHBYTES('SHA2_256', CustomSQL)) AS QueryHash
	INTO #Queries
	FROM etl.AuditCustomRule
	WHERE Active=1

	IF EXISTS (
		SELECT 1/0
		FROM #Queries
		WHERE QueryHash = @QueryHash
	)
	BEGIN
		SET @ErrorMessage = 'There is already an active custom rule with this query';
		THROW 51000, @ErrorMessage, 1;
	END

	INSERT INTO etl.AuditCustomRule (PipelineID, CustomSQL, CustomAlertMessage, InfoAlert, ErrorAlert, RuleStartDate, RuleEndDate)
	VALUES (
	@PipelineID,
	@CustomSQL,
	@CustomAlertMessage,
	@InfoAlert,
	@ErrorAlert,
	@RuleStartDate,
	@RuleEndDate
	)

END
GO


DROP PROCEDURE IF EXISTS etl.AuditCustomCheck
GO
CREATE PROCEDURE etl.AuditCustomCheck @ParentRunTimeID BIGINT = NULL,
									  @RunTimeID BIGINT,
									  @PipelineID INT,
									  @Debug BIT = 0
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DROP TABLE IF EXISTS #CustomRules
	;WITH CTE AS (
	SELECT
	AuditCustomRuleID
	FROM etl.AuditCustomRule
	WHERE PipelineID = @PipelineID
		AND Active = 1
	)
	SELECT
	a.AuditCustomRuleID,
	a.PipelineID,
	p.PipelineName,
	a.CustomSQL,
	a.CustomAlertMessage,
	a.InfoAlert,
	a.ErrorAlert,
	0 AS Complete /* For cursor work */
	INTO #CustomRules
	FROM etl.AuditCustomRule AS a
	INNER JOIN CTE
		ON CTE.AuditCustomRuleID = a.AuditCustomRuleID
	INNER JOIN etl.Pipeline AS p
		ON p.PipelineID = a.PipelineID

	DROP TABLE IF EXISTS #FailResults
	CREATE TABLE #FailResults (
		PipelineID INT,
		PipelineName VARCHAR(150),
		AuditCustomRuleID INT,
		CustomAlertMessage VARCHAR(400),
		InfoAlert BIT,
		ErrorAlert BIT
	)

	DECLARE @AuditCustomRuleID INT, 
			@PipelineName VARCHAR(150), 
			@SQLQuery NVARCHAR(MAX), 
			@CustomAlertMessage VARCHAR(400),
			@InfoAlert BIT,
			@ErrorAlert BIT,
			@Success BIT
	DECLARE custom_cursor CURSOR FOR
		SELECT
		AuditCustomRuleID,
		PipelineName,
		CustomSQL,
		CustomALertMessage,
		InfoAlert,
		ErrorAlert
		FROM #CustomRules
		WHERE Complete = 0
		ORDER BY AuditCustomRuleID

	OPEN custom_cursor
	FETCH NEXT FROM custom_cursor INTO
	@AuditCustomRuleID, @PipelineName, @SQLQuery, @CustomAlertMessage, @InfoAlert, @ErrorAlert

	WHILE @@FETCH_STATUS = 0
	BEGIN
		SET @Success = NULL
		
		IF @Debug = 1
		BEGIN
			PRINT @SQLQuery
		END
		ELSE
		BEGIN

			INSERT INTO etl.AuditCustomLog (RunTimeID, AuditCustomRuleID, CustomQueryStart)
			VALUES (@RunTimeID, @AuditCustomRuleID, SYSDATETIMEOFFSET())

			SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
			EXEC sys.sp_executesql @SQLQuery, 
				N'@Success BIT OUTPUT', 
				@Success = @Success OUTPUT
			SET TRANSACTION ISOLATION LEVEL READ COMMITTED

			IF @Success IS NULL
				SET @Success = 0

			;WITH CTE AS (
			SELECT
			AuditCustomLogID
			FROM etl.AuditCustomLog
			WHERE RunTimeID = @RunTimeID
				AND AuditCustomRuleID = @AuditCustomRuleID
			)
			UPDATE acl
			SET CustomQueryEnd = SYSDATETIMEOFFSET(),
				RunTimeSeconds = DATEDIFF(SECOND, CustomQueryStart, SYSDATETIMEOFFSET()),
				Completed = 1,
				Success = @Success
			FROM etl.AuditCustomLog acl
			INNER JOIN CTE 
				ON CTE.AuditCustomLogID = acl.AuditCustomLogID

		END

		IF (@Success = 0)
		BEGIN
			INSERT INTO #FailResults (PipelineID, AuditCustomRuleID, PipelineName, CustomAlertMessage, InfoAlert, ErrorAlert)
			VALUES (@PipelineID, @AuditCustomRuleID, @PipelineName, @CustomAlertMessage, @InfoAlert, @ErrorAlert)
		END
		
		UPDATE #CustomRules
		SET Complete=1
		WHERE AuditCustomRuleID = @AuditCustomRuleID

		FETCH NEXT FROM custom_cursor INTO
		@AuditCustomRuleID, @PipelineName, @SQLQuery, @CustomAlertMessage, @InfoAlert, @ErrorAlert

	END

	CLOSE custom_cursor
	DEALLOCATE custom_cursor

	DROP TABLE #CustomRules

	IF EXISTS (SELECT 1/0 FROM #FailResults WHERE InfoAlert = 1)
	BEGIN

		DECLARE @InfoMessage VARCHAR(4000) = 'The following custom rules for ' + CAST(@PipelineID AS VARCHAR(50)) + ': ''' + @PipelineName + ''' have failed info checks:' + CHAR(13) + ''
		SELECT @InfoMessage += STRING_AGG('AuditCustomRuleID: ' + CAST(AuditCustomRuleID AS VARCHAR(50)) + ' - CustomInfoMessage: ' + CAST(CustomAlertMessage AS VARCHAR(400)) + '', CHAR(13))
		FROM #FailResults
		WHERE InfoAlert = 1

		/* TODO: Configure Database Mail */
		PRINT @InfoMessage
	END

	IF EXISTS (SELECT 1/0 FROM #FailResults WHERE ErrorAlert = 1)
	BEGIN

		DECLARE @ErrorMessage VARCHAR(4000) = 'The following custom rules for ' + CAST(@PipelineID AS VARCHAR(50)) + ': ''' + @PipelineName + ''' have failed error checks:' + CHAR(13) + ''
		SELECT @ErrorMessage += STRING_AGG('AuditCustomRuleID: ' + CAST(AuditCustomRuleID AS VARCHAR(50)) + ' - CustomErrorMessage: ' + CAST(CustomAlertMessage AS VARCHAR(400)) + '', CHAR(13))
		FROM #FailResults
		WHERE InfoAlert = 1;

		/* TODO: Configure Database Mail */
		
		/* Throw alert to stop pipeline */
		THROW 51000, @ErrorMessage, 1;

	END
END
GO


DROP PROCEDURE IF EXISTS etl.DeprecationFinal_U
GO
CREATE PROCEDURE etl.DeprecationFinal_U @FullAddress VARCHAR(150)
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON
	DECLARE @AddressID INT,
			@Message VARCHAR(100)

	SELECT
	@AddressID = AddressID
	FROM etl.Address
	WHERE FullAddress = @FullAddress

	IF @AddressID IS NULL
	BEGIN
		SET @Message = 'Address ''' + @FullAddress + ''' is unable to be found.';
		THROW 51000, @Message, 1;
	END

	IF NOT EXISTS (
		SELECT 1/0
		FROM etl.DeprecationDeclared
		WHERE AddressID = @AddressID
	)
	BEGIN
		SET @Message = 'Address ''' + @FullAddress + ''' is unable to be found in etl.DeprecationDeclared';
		THROW 51000, @Message, 1;
	END

	UPDATE etl.DeprecationDeclared
	SET TicketsCreated = 1
	WHERE AddressID = @AddressID

	SET @Message = 'Address ''' + @FullAddress + ''' marked as TicketsCreated = 1'
	PRINT @Message

	SELECT
	'etl.DeprecationDeclared' AS TableName,
	*
	FROM etl.DeprecationDeclared
	WHERE AddressID = @AddressID

END
GO


DROP PROCEDURE IF EXISTS etl.AddressLineage_S
GO
CREATE PROCEDURE etl.AddressLineage_S @FullAddress VARCHAR(150),
									  @Upstream BIT,
									  @Downstream BIT
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DECLARE @AddressID INT,
			@ErrorMessage VARCHAR(100)

	IF (@Upstream IS NULL AND @Downstream IS NULL) OR (@Upstream=0 AND @Downstream=0) OR (@Upstream=1 AND @Downstream=1)
	BEGIN
		SET @ErrorMessage = 'Must declare @Upstream and @Downstream variables, one must be active';
		THROW 51000, @ErrorMessage, 1;
	END
	
	SELECT @AddressID = AddressID
	FROM etl.Address
	WHERE FullAddress = @FullAddress

	IF @AddressID IS NULL
	BEGIN
		SET @ErrorMessage = 'Address ''' + @FullAddress + '''' + ' is unable to be found.';
		THROW 51000, @ErrorMessage, 1;
	END

	/* Begin script */
	/* Ensure lineage is also grabbed from 1-to-1 pipelines */
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
	DROP TABLE IF EXISTS #Pipelines
	SELECT
	SourceAddressID,
	TargetAddressID
	INTO #Pipelines
	FROM etl.Pipeline
	WHERE SourceAddressID IS NOT NULL
		AND TargetAddressID IS NOT NULL

	INSERT INTO etl.AddressDependency (SourceAddressID, TargetAddressID)
	SELECT
	SourceAddressID,
	TargetAddressID
	FROM #Pipelines AS p
	WHERE NOT EXISTS (
		SELECT 1/0
		FROM etl.AddressDependency AS ad
		WHERE ad.TargetAddressID  = p.TargetAddressID
			AND ad.SourceAddressID = p.SourceAddressID
		)

	DROP TABLE #Pipelines

	DROP TABLE IF EXISTS #Results
	CREATE TABLE #Results (
		Level INT,
		TargetAddressID INT,
		SourceAddressID INT
	)

	IF @Upstream = 1
	BEGIN
		;WITH RecursiveCTE AS (
			SELECT
			0 AS Level,
			Anchor.TargetAddressID,
			Anchor.SourceAddressID
			FROM etl.AddressDependency AS Anchor
			WHERE TargetAddressID = @AddressID
			UNION ALL
			SELECT
			RecursiveCTE.Level - 1,
			Recursion.TargetAddressID,
			Recursion.SourceAddressID
			FROM etl.AddressDependency AS Recursion
			INNER JOIN RecursiveCTE
				ON Recursion.TargetAddressID = RecursiveCTE.SourceAddressID
		)

		INSERT INTO #Results (Level, TargetAddressID, SourceAddressID)
		SELECT
		Level,
		TargetAddressID,
		SourceAddressID
		FROM RecursiveCTE

		IF EXISTS (SELECT 1/0 FROM #Results)
		BEGIN
			SELECT
			r.Level,
			t.FullAddress AS TargetAddress,
			tt.AddressType AS TargetAddressType,
			'<<<<<' AS [<<<<<],
			s.FullAddress AS SourceAddress,
			st.AddressType AS SourceAddressType
			FROM #Results AS r
			INNER JOIN etl.Address AS s
				ON s.AddressID = r.SourceAddressID
			INNER JOIN etl.AddressType AS st
				ON st.AddressTypeID = s.AddressTypeID
			INNEr JOIN etl.Address AS t
				ON t.AddressID = r.TargetAddressID
			INNER JOIN etl.AddressType AS tt
				ON tt.AddressTypeID = t.AddressTypeID
		END
		ELSE
		BEGIN
			SET @ErrorMessage = 'INFO: Address ''' + @FullAddress + ''' has no upstream dependencies'
			RAISERROR(@ErrorMessage,0,1) WITH NOWAIT;
		END
	END

	IF @Downstream = 1
	BEGIN
		;WITH RecursiveCTE AS (
			SELECT
			0 AS Level,
			Anchor.SourceAddressID,
			Anchor.TargetAddressID
			FROM etl.AddressDependency AS Anchor
			WHERE SourceAddressID = @AddressID
			UNION ALL
			SELECT
			RecursiveCTE.Level + 1,
			Recursion.SourceAddressID,
			Recursion.TargetAddressID
			FROM etl.AddressDependency AS Recursion
			INNER JOIN RecursiveCTE
				ON Recursion.SourceAddressID = RecursiveCTE.TargetAddressID
		)

		INSERT INTO #Results (Level, SourceAddressID, TargetAddressID)
		SELECT
		Level,
		SourceAddressID,
		TargetAddressID
		FROM RecursiveCTE

		IF EXISTS (SELECT 1/0 FROM #Results)
		BEGIN
			SELECT
			r.Level,
			s.FullAddress AS SourceAddress,
			st.AddressType AS SourceAddressType,
			'>>>>>' AS [>>>>>],
			t.FullAddress AS TargetAddress,
			tt.AddressType AS TargetAddressType
			FROM #Results AS r
			INNER JOIN etl.Address AS s
				ON s.AddressID = r.SourceAddressID
			INNER JOIN etl.AddressType AS st
				ON st.AddressTypeID = s.AddressTypeID
			INNEr JOIN etl.Address AS t
				ON t.AddressID = r.TargetAddressID
			INNER JOIN etl.AddressType AS tt
				ON tt.AddressTypeID = t.AddressTypeID
		END
		ELSE
		BEGIN
			SET @ErrorMessage = 'INFO: Address ''' + @FullAddress + ''' has no downstream dependencies'
			RAISERROR(@ErrorMessage,0,1) WITH NOWAIT;
		END
	END
END
GO