USE [ETL_Metadata]
GO
/****** Object:  StoredProcedure [etl].[Pipeline_UI]    Script Date: 1/16/2024 10:18:49 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
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