USE ETL_Metadata
GO

DECLARE @ParentRunTimeID BIGINT
SELECT @ParentRunTimeID = MAX(RunTimeID)
FROM etl.RunTime AS pe
INNER JOIN etl.Pipeline AS p
	ON p.PipelineID = pe.PipelineID
WHERE p.PipelineName = 'Test Controller Source Sync'

SELECT 
pe.ParentRunTimeID,
pe.RunTimeID,
p.PipelineID,
p.PipelineName,
pt.PipelineType,
pe.PipelineStart,
pe.PipelineEnd,
pe.RunTimeSeconds,
pe.Completed,
pe.Inserts,
pe.Updates,
pe.SoftDeletes,
pe.TotalRows,
pe.FullLoad,
pe.WaterMark,
pe.NextWaterMark
FROM etl.RunTime AS pe
INNER JOIN etl.Pipeline AS p
	ON p.PipelineID = pe.PipelineID
INNER JOIN etl.PipelineType AS pt
	ON pt.PipelineTypeID = p.PipelineTypeID
WHERE ISNULL(ParentRunTimeId, pe.RunTimeID) >= @ParentRunTimeID

SELECT
al.AuditColumnLogID,
ar.AuditColumnRuleID,
aut.AuditType,
ar.AuditColumn,
ar.MinimumBound,
ar.MaximumBound,
ar.LookbackDays,
ar.StdDeviationFactor,
al.HourRecorded,
al.RecordCount,
al.RecordViolationCount,
al.ResultScore,
al.Threshold,
al.Success,
ar.InfoAlert,
ar.ErrorAlert,
al.FullLoad,
al.ChunkOut
FROM  etl.AuditColumnRule AS ar
INNER JOIN etl.AuditColumnLog AS al
	ON al.AuditColumnLogID = ar.LastAuditColumnLogID
INNER JOIN etl.AuditType AS aut
	ON aut.AuditTypeID = ar.AuditTypeID

/*
update etl.Pipeline
SET PipelineArgs = '{"info": {"audit" : {"batch_size": 300000, "chunkout": 1, "samples_needed": 7}}}'
WHERE PipelineID = 3

update etl.Pipeline
SET PipelineArgs = '{"info": {"publish": {"batch_size": 500000}}}'
WHERE PipelineID = 4
*/