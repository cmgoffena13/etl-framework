USE ETL_Metadata
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
		SET @ErrorMessage = 'No Pipelines in etl.DeprecationDeclared to be deprecated, no need to run';
		THROW 51000, @ErrorMessage, 1;
	END

	/* Proceed to delete Pipelines associated with address marked for deprecation */
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

	/* Update any remaining Pipeline source address id to be null instead */
	UPDATE t
	SET SourceAddressID = NULL
	FROM etl.Pipeline AS t
	WHERE SourceAddressID = @AddressID

	/* After all Pipeline records and logs are deleted, delete address */
	DELETE FROM etl.Address
	WHERE AddressID = @AddressID
		AND Deprecated=1

	/* Remove marked deprecation records, revert not possible anymore */
	DELETE FROM etl.DeprecationDeclared
	WHERE AddressID = @AddressID

	PRINT 'All data associated with Address ''' + @FullAddress + ''' has been removed.'

END