USE ETL_Metadata
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