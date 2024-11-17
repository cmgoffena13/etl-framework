THROW 51000, 'Make sure to designate the right database and then comment this out', 1;

/* Create test Schema */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'test')
BEGIN
	EXEC('CREATE SCHEMA test AUTHORIZATION dbo')
END

DROP TABLE IF EXISTS test.SourceTable
CREATE TABLE test.SourceTable (
	EventID BIGINT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_test_SourceTable_EventID PRIMARY KEY,
	EventValue DECIMAL(17,2) NOT NULL,
	EventReason VARCHAR(50) NOT NULL,
	CreatedDate DATETIMEOFFSET(2) NOT NULL
)
ALTER TABLE test.SourceTable
	ENABLE CHANGE_TRACKING

DROP TABLE IF EXISTS test.TargetStage
CREATE TABLE test.TargetStage (
	TargetStageID INT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_test_TargetStage_TargetStageID PRIMARY KEY,
	EventID BIGINT,
	EventValue DECIMAL(17,2),
	EventReason VARCHAR(50),
	CreatedDate DATETIMEOFFSET(2),
	CTOperation CHAR(1) NOT NULL,
	CTOperationContext VARCHAR(128),
	RowHash VARBINARY(32) NOT NULL,
	RunTimeID BIGINT NOT NULL
)
CREATE NONCLUSTERED INDEX IX_test_TargetStage_RunTimeID
	ON test.TargetStage (RunTimeID)

DROP TABLE IF EXISTS test.TargetPublish
CREATE TABLE test.TargetPublish (
	RowKey BIGINT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_test_TargetPublish_RowKey PRIMARY KEY,
	EventID BIGINT,
	EventValue DECIMAL(17,2),
	EventReason VARCHAR(50),
	CreatedDate DATETIMEOFFSET(2),
	RowHash VARBINARY(32) NOT NULL,
	CreatedOn DATETIMEOFFSET(2) NOT NULL
		CONSTRAINT DF_test_TargetPublish_CreatedOn DEFAULT(SYSDATETIMEOFFSET()),
	CreatedBy INT NOT NULL
		CONSTRAINT DF_test_TargetPublish_CreatedBy DEFAULT(-1),
	ModifiedOn DATETIMEOFFSET(2) NULL,
	ModifiedBy INT NULL,
	WaterMarkDate DATETIMEOFFSET(2) NOT NULL
		CONSTRAINT DF_test_TargetPublish_WaterMarkDate DEFAULT(SYSDATETIMEOFFSET()),
	RunTimeID BIGINT NOT NULL,
	ActiveInSourceSystem BIT NOT NULL
		CONSTRAINT DF_test_TargetPublish_ActiveInSourceSystem DEFAULT(1)
)
CREATE NONCLUSTERED INDEX IX_test_TargetPublish_EventID
	ON test.TargetPublish (EventID)
CREATE NONCLUSTERED INDEX IX_test_TargetPublish_RunTimeID
	ON test.TargetPublish (RunTimeID)


DROP TABLE IF EXISTS test.SourceTable2
CREATE TABLE test.SourceTable2 (
	SaleID BIGINT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_test_SourceTable2_SaleID PRIMARY KEY,
	Revenue DECIMAL(17,2) NOT NULL,
	LastSaleEventReason VARCHAR(50) NOT NULL,
	CreatedDate DATETIMEOFFSET(2) NOT NULL
)
ALTER TABLE test.SourceTable2
	ENABLE CHANGE_TRACKING


DROP TABLE IF EXISTS test.TargetStage2
CREATE TABLE test.TargetStage2 (
	TargetStage2ID INT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_test_TargetStage2_TargetStage2ID PRIMARY KEY,
	SaleID BIGINT,
	Revenue DECIMAL(17,2),
	LastSaleEventReason VARCHAR(50),
	CreatedDate DATETIMEOFFSET(2),
	CTOperation CHAR(1) NOT NULL,
	CTOperationContext VARCHAR(128),
	RowHash VARBINARY(32) NOT NULL,
	RunTimeID BIGINT NOT NULL
)
CREATE NONCLUSTERED INDEX IX_test_TargetStage2_RunTimeID
	ON test.TargetStage2 (RunTimeID)

DROP TABLE IF EXISTS test.TargetPublish2
CREATE TABLE test.TargetPublish2 (
	RowKey BIGINT NOT NULL IDENTITY(1,1)
		CONSTRAINT PK_test_TargetPublish2_RowKey PRIMARY KEY,
	SaleID BIGINT,
	Revenue DECIMAL(17,2),
	LastSaleEventReason VARCHAR(50),
	CreatedDate DATETIMEOFFSET(2),
	RowHash VARBINARY(32) NOT NULL,
	CreatedOn DATETIMEOFFSET(2) NOT NULL
		CONSTRAINT DF_test_TargetPublish2_CreatedOn DEFAULT(SYSDATETIMEOFFSET()),
	CreatedBy INT NOT NULL
		CONSTRAINT DF_test_TargetPublish2_CreatedBy DEFAULT(-1),
	ModifiedOn DATETIMEOFFSET(2) NULL,
	ModifiedBy INT NULL,
	WaterMarkDate DATETIMEOFFSET(2) NOT NULL
		CONSTRAINT DF_test_TargetPublish2_WaterMarkDate DEFAULT(SYSDATETIMEOFFSET()),
	RunTimeID BIGINT NOT NULL,
	ActiveInSourceSystem BIT NOT NULL
		CONSTRAINT DF_test_TargetPublish_ActiveInSourceSystem DEFAULT(1)
)
CREATE NONCLUSTERED INDEX IX_test_TargetPublish2_SaleID
	ON test.TargetPublish2 (SaleID)
CREATE NONCLUSTERED INDEX IX_test_TargetPublish2_RunTimeID
	ON test.TargetPublish2 (RunTimeID)