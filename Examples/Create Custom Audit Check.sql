USE ETL_Metadata
GO

DECLARE @PipelineID INT
EXEC etl.Pipeline_UI	@PipelineName = 'Test Custom Audit SQL Pipeline',
					@PipelineType = 'Audit',
					@TargetFullAddress = 'ETL_Metadata.test.TargetStage',
					@TargetAddressType = 'Database',
					@PipelineID = @PipelineID OUTPUT

EXEC etl.AuditCustomRule_I @PipelineName = 'Test Custom Audit SQL Pipeline',
@CustomSQL = N'
DECLARE @Success BIT = 
(
SELECT CASE WHEN COUNT(*)=0 THEN 1 ELSE 0 END
FROM ETL_Metadata.test.TargetStage
)
SELECT @Success
'
,
					 @CustomAlertMessage = 'This is a custom alert message!',
					 @InfoAlert = 1,
					 @ErrorAlert = 0

SELECT * FROM etl.AuditCustomRule