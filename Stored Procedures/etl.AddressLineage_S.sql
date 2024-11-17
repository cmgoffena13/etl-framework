USE ETL_Metadata
GO


DROP PROCEDURE IF EXISTS etl.AddressLineage_S
GO
CREATE PROCEDURE etl.AddressLineage_S @FullAddress VARCHAR(150),
									  @Upstream BIT,
									  @Downstream BIT
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON

	DECLARE @AddressID INT,
			@ErrorMessage VARCHAR(100)

	IF (@Upstream IS NULL AND @Downstream IS NULL) OR (@Upstream=0 AND @Downstream=0) OR (@Upstream=1 AND @Downstream=1)
	BEGIN
		SET @ErrorMessage = 'Must declare @Upstream and @Downstream variables, one must be active';
		THROW 51000, @ErrorMessage, 1;
	END
	
	SELECT @AddressID = AddressID
	FROM etl.Address
	WHERE FullAddress = @FullAddress

	IF @AddressID IS NULL
	BEGIN
		SET @ErrorMessage = 'Address ''' + @FullAddress + '''' + ' is unable to be found.';
		THROW 51000, @ErrorMessage, 1;
	END

	/* Begin script */
	/* Ensure lineage is also grabbed from 1-to-1 pipelines */
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
	DROP TABLE IF EXISTS #Pipelines
	SELECT
	SourceAddressID,
	TargetAddressID
	INTO #Pipelines
	FROM etl.Pipeline
	WHERE SourceAddressID IS NOT NULL
		AND TargetAddressID IS NOT NULL

	INSERT INTO etl.AddressDependency (SourceAddressID, TargetAddressID)
	SELECT
	SourceAddressID,
	TargetAddressID
	FROM #Pipelines AS p
	WHERE NOT EXISTS (
		SELECT 1/0
		FROM etl.AddressDependency AS ad
		WHERE ad.TargetAddressID  = p.TargetAddressID
			AND ad.SourceAddressID = p.SourceAddressID
		)

	DROP TABLE #Pipelines

	DROP TABLE IF EXISTS #Results
	CREATE TABLE #Results (
		Level INT,
		TargetAddressID INT,
		SourceAddressID INT
	)

	IF @Upstream = 1
	BEGIN
		;WITH RecursiveCTE AS (
			SELECT
			0 AS Level,
			Anchor.TargetAddressID,
			Anchor.SourceAddressID
			FROM etl.AddressDependency AS Anchor
			WHERE TargetAddressID = @AddressID
			UNION ALL
			SELECT
			RecursiveCTE.Level - 1,
			Recursion.TargetAddressID,
			Recursion.SourceAddressID
			FROM etl.AddressDependency AS Recursion
			INNER JOIN RecursiveCTE
				ON Recursion.TargetAddressID = RecursiveCTE.SourceAddressID
		)

		INSERT INTO #Results (Level, TargetAddressID, SourceAddressID)
		SELECT
		Level,
		TargetAddressID,
		SourceAddressID
		FROM RecursiveCTE

		IF EXISTS (SELECT 1/0 FROM #Results)
		BEGIN
			SELECT
			r.Level,
			t.FullAddress AS TargetAddress,
			tt.AddressType AS TargetAddressType,
			'<<<<<' AS [<<<<<],
			s.FullAddress AS SourceAddress,
			st.AddressType AS SourceAddressType
			FROM #Results AS r
			INNER JOIN etl.Address AS s
				ON s.AddressID = r.SourceAddressID
			INNER JOIN etl.AddressType AS st
				ON st.AddressTypeID = s.AddressTypeID
			INNEr JOIN etl.Address AS t
				ON t.AddressID = r.TargetAddressID
			INNER JOIN etl.AddressType AS tt
				ON tt.AddressTypeID = t.AddressTypeID
		END
		ELSE
		BEGIN
			SET @ErrorMessage = 'INFO: Address ''' + @FullAddress + ''' has no upstream dependencies'
			RAISERROR(@ErrorMessage,0,1) WITH NOWAIT;
		END
	END

	IF @Downstream = 1
	BEGIN
		;WITH RecursiveCTE AS (
			SELECT
			0 AS Level,
			Anchor.SourceAddressID,
			Anchor.TargetAddressID
			FROM etl.AddressDependency AS Anchor
			WHERE SourceAddressID = @AddressID
			UNION ALL
			SELECT
			RecursiveCTE.Level + 1,
			Recursion.SourceAddressID,
			Recursion.TargetAddressID
			FROM etl.AddressDependency AS Recursion
			INNER JOIN RecursiveCTE
				ON Recursion.SourceAddressID = RecursiveCTE.TargetAddressID
		)

		INSERT INTO #Results (Level, SourceAddressID, TargetAddressID)
		SELECT
		Level,
		SourceAddressID,
		TargetAddressID
		FROM RecursiveCTE

		IF EXISTS (SELECT 1/0 FROM #Results)
		BEGIN
			SELECT
			r.Level,
			s.FullAddress AS SourceAddress,
			st.AddressType AS SourceAddressType,
			'>>>>>' AS [>>>>>],
			t.FullAddress AS TargetAddress,
			tt.AddressType AS TargetAddressType
			FROM #Results AS r
			INNER JOIN etl.Address AS s
				ON s.AddressID = r.SourceAddressID
			INNER JOIN etl.AddressType AS st
				ON st.AddressTypeID = s.AddressTypeID
			INNEr JOIN etl.Address AS t
				ON t.AddressID = r.TargetAddressID
			INNER JOIN etl.AddressType AS tt
				ON tt.AddressTypeID = t.AddressTypeID
		END
		ELSE
		BEGIN
			SET @ErrorMessage = 'INFO: Address ''' + @FullAddress + ''' has no downstream dependencies'
			RAISERROR(@ErrorMessage,0,1) WITH NOWAIT;
		END
	END
END