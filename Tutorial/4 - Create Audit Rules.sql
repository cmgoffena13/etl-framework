THROW 51000, 'Make sure to designate the right database in @TargetFullAddress and then comment this out', 1;


/* Create the Pipeline, need target address record for audits */
DECLARE @PipelineID INT
EXEC etl.Pipeline_UI	@PipelineName = 'Test Incremental Audit SQL Pipeline',
					@PipelineType = 'Audit',
					@PipelineArgs = '{"info": {"audit" : {"batch_size": 300000, "chunkout": 1, "samples_needed": 7}}}',
					@TargetFullAddress = 'ETL_Metadata.test.TargetStage',
					@TargetAddressType = 'Database',
					@PipelineID = @PipelineID

/* Create a Completeness rule */
EXEC etl.AuditColumnRule_I @PipelineName = 'Test Incremental Audit SQL Pipeline',
						   @AuditType = 'Completeness',
						   @PrimaryKey = 'TargetStageID',
						   @AuditColumn = 'EventID',
						   @InfoAlert = 1,
						   @ErrorAlert = 0

/* Create an Accuracy rule */
EXEC etl.AuditColumnRule_I @PipelineName = 'Test Incremental Audit SQL Pipeline',
						   @AuditType = 'Accuracy',
						   @PrimaryKey = 'TargetStageID',
						   @AuditColumn = 'EventValue',
						   @MinimumBound = 0,
						   @MaximumBound = 1500,
						   @InfoAlert = 1,
						   @ErrorAlert = 0

SELECT * FROM etl.AuditColumnRule

/* Create a custom sql for custom rule */
DECLARE @CustomSQL NVARCHAR(MAX) = '
SELECT 
@Success = CASE WHEN COUNT(*)=0 THEN 1 ELSE 0 END 
FROM test.TargetStage
WHERE EventValue > 1000000
'

/* Create custom rule */
EXEC etl.AuditCustomRule_I @PipelineName = 'Test Incremental Audit SQL Pipeline',
						   @CustomSQL = @CustomSQL,
						   @CustomAlertMessage = 'EventValue greater than 1 million found',
						   @InfoAlert = 1,
						   @ErrorAlert = 0

SELECT * FROM etl.AuditCustomRule