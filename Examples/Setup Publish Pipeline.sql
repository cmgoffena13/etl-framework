USE ETL_Metadata
GO
DECLARE @PipelineID INT
EXEC etl.Pipeline_UI	@PipelineName = 'Test Incremental Publish SQL Pipeline',
					@PipelineType = 'Publish',
					@PipelineArgs = '{"info": { "publish": {"batch_size": 300000 }}}',
					@TargetFullAddress = 'ETL_Metadata.test.TargetPublish',
					@PipelineID = @PipelineID