USE ETL_Metadata
GO
DECLARE @PipelineID INT
EXEC etl.Pipeline_UI	@PipelineName = 'Test Incremental Audit SQL Pipeline',
					@PipelineType = 'Audit',
					@PipelineArgs = '{"info": {"audit" : {"batch_size": 300000, "chunkout": 1, "samples_needed": 7}}}',
					@TargetFullAddress = 'ETL_Metadata.test.TargetStage',
					@PipelineID = @PipelineID

EXEC etl.AuditColumnRule_I @PipelineName = 'Test Incremental Audit SQL Pipeline',
						   @AuditType = 'Completeness',
						   @DatabaseName = 'ETL_Metadata',
						   @SchemaName = 'test',
						   @TableName = 'TargetStage',
						   @PrimaryKey = 'TargetStageID',
						   @AuditColumn = 'EventID',
						   @InfoAlert = 1,
						   @ErrorAlert = 0

EXEC etl.AuditColumnRule_I @PipelineName = 'Test Incremental Audit SQL Pipeline',
						   @AuditType = 'Accuracy',
						   @DatabaseName = 'ETL_Metadata',
						   @SchemaName = 'test',
						   @TableName = 'TargetStage',
						   @PrimaryKey = 'TargetStageID',
						   @AuditColumn = 'CreatedDate',
						   @MinimumBound = NULL,
						   @MaximumBound = 'SYSDATETIMEOFFSET()',
						   @InfoAlert = 1,
						   @ErrorAlert = 0

SELECT * FROM etl.Pipeline WHERE PipelineName = 'Test Incremental Audit SQL Pipeline'
UPDATE etl.Pipeline
SET PipelineArgs = '{"info": {"audit" : {"batch_size": 300000, "chunkout": 1, "samples_needed": 7}}}'
WHERE PipelineName =  'Test Incremental Audit SQL Pipeline'
SELECT TOP 1 * FROM test.TargetStage
SELECT * FROM etl.AuditColumnRule