USE ETL_Metadata
GO

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

DROP TABLE IF EXISTS etl.PipelineArgsLog
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
