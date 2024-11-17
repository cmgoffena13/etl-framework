USE ETL_Metadata
GO


DROP PROCEDURE IF EXISTS etl.DeprecationDeclared_UID
GO
CREATE PROCEDURE etl.DeprecationDeclared_UID @FullAddress VARCHAR(150),
											@Revert BIT = 0,
											@RenameTable BIT = 1,
											@Debug BIT = 0
AS
BEGIN
SET NOCOUNT ON
SET XACT_ABORT ON
	DECLARE @ErrorMessage VARCHAR(100),
			@SQLQuery NVARCHAR(MAX),
			@AddressTypeID INT,
			@AddressType VARCHAR(150),
			@AddressID INT,
			@DatabaseName VARCHAR(50),
			@SchemaName VARCHAR(50),
			@TableName VARCHAR(50),
			@Message VARCHAR(150)

	DECLARE @CatchErrorMessage NVARCHAR(MAX),
			@ErrorSeverity INT,
			@ErrorState INT

	;WITH CTE AS (
		SELECT
		AddressID
		FROM etl.Address
		WHERE FullAddress = @FullAddress
	)
	SELECT
	@AddressID = a.AddressID,
	@AddressTypeID = a.AddressTypeID,
	@DatabaseName = a.DatabaseName,
	@SchemaName = a.SchemaName,
	@TableName = a.TableName
	FROM etl.Address AS a
	INNER JOIN CTE
		ON CTE.AddressID = a.AddressID

	SELECT
	@AddressType = AddressType
	FROM etl.AddressType
	WHERE AddressTypeID = @AddressTypeID

	IF @AddressID IS NULL
	BEGIN
		SET @ErrorMessage = 'Address: ''' + @FullAddress + ''' unable to be found';
		THROW 51000, @ErrorMessage, 1;
	END

	/* Grab Pipelines associated with address to de-activate */
	DROP TABLE IF EXISTS #Pipelines
	CREATE TABLE #Pipelines (PipelineID INT)
	INSERT INTO #Pipelines (PipelineID)
	SELECT
	PipelineID
	FROM etl.Pipeline AS p
	INNER JOIN etl.Address AS s
		ON s.AddressID = p.SourceAddressID
	UNION 
	SELECT
	PipelineID
	FROM etl.Pipeline AS p
	INNER JOIN etl.Address AS t
		ON t.AddressID = p.TargetAddressID

	/* Grab first layer of dependencies for address */
	DROP TABLE IF EXISTS #AddressLineageAffected
	CREATE TABLE #AddressLineageAffected ( AddressID INT )
	INSERT INTO #AddressLineageAffected (AddressID)
	SELECT
	TargetAddressID AS AddressID
	FROM etl.AddressDependency AS ad
	WHERE TargetAddressID = @AddressID
	UNION
	SELECT
	SourceAddressID AS AddressID
	FROM etl.AddressDependency AS ad
	WHERE SourceAddressID = @AddressID

	/* Grab all Pipelines associated with address dependecies */
	DROP TABLE IF EXISTS #PipelineLineageAffected
	CREATE TABLE #PipelineLineageAffected (PipelineID INT)
	INSERT INTO #PipelineLineageAffected (PipelineID)
	SELECT
	PipelineID
	FROM etl.Pipeline AS p
	INNER JOIN #AddressLineageAffected AS s
		ON s.AddressID = p.SourceAddressID
	UNION
	SELECT
	PipelineID
	FROM etl.Pipeline AS p
	INNER JOIN #AddressLineageAffected AS t
		ON t.AddressID = p.TargetAddressID

	/* Set all Pipelines to reload */
	/* We'll delete the target lineage */
	/* When physical objects are removed the lineage will be checked again to ensure its accurate */
	/* Can't remove the data in the future if the address is found in the dependency table */
	UPDATE t
	SET LoadLineage = 1
	FROM etl.Pipeline AS t
	INNER JOIN #PipelineLineageAffected AS p
		ON p.PipelineID = t.PipelineID

	DROP TABLE #AddressLineageAffected
	DROP TABLE #PipelineLineageAffected

	DECLARE @DeprecationTable TABLE (
		AddressID INT,
		ID INT,
		IDType VARCHAR(150),
		IDName VARCHAR(150)
	)

	IF @Revert = 0
	BEGIN
		BEGIN TRY
			BEGIN TRANSACTION

			/* Delete any dependencies */
			/* We flagged lineage reload to check that lineage records are accurate */
			DELETE FROM etl.AddressDependency
			WHERE TargetAddressID = @AddressID

			/* Update associated Pipelines to be inactive */
			UPDATE t
			SET Active = 0,
				MuteTimelyCheck = 0
			OUTPUT @AddressID, deleted.PipelineID, 'PipelineID', deleted.PipelineName
			INTO @DeprecationTable
			FROM etl.Pipeline AS t
			INNER JOIN #Pipelines AS p
				ON p.PipelineID = t.PipelineID

			/* Update address to have deprecated flag */
			UPDATE etl.Address
			SET Deprecated = 1
			WHERE AddressID = @AddressID

			/* Update audit rules to be inactive */
			UPDATE t
			SET RuleEndDate = CAST(SYSDATETIMEOFFSET() AS DATE),
				Active = 0
			OUTPUT @AddressID, deleted.AuditColumnRuleID, 'AuditColumnRuleID', deleted.AuditColumn
			INTO @DeprecationTable
			FROM etl.AuditColumnRule AS t
			INNER JOIN #Pipelines AS p
				ON p.PipelineID = t.PipelineID
			WHERE t.Active=1

			UPDATE t
			SET RuleEndDate = CAST(SYSDATETIMEOFFSET() AS DATE),
				Active = 0
			OUTPUT @AddressID, deleted.AuditCustomRuleID, 'AuditCustomRuleID', NULL
			INTO @DeprecationTable
			FROM etl.AuditCustomRule AS t
			INNER JOIN #Pipelines AS p
				ON p.PipelineID = t.PipelineID
			WHERE t.Active=1

			/* Rename TableName to TableName_Deprecated if flagged */
			IF @AddressType = 'Database' AND DB_NAME() = @DatabaseName AND @RenameTable = 1
			BEGIN
				SET @SQLQuery = 'EXEC sp_rename ''' + @SchemaName + '.' + @TableName + ''', ''' + @TableName + '_Deprecated' + ''';'
				IF @Debug = 1
				BEGIN
					PRINT @SQLQuery
				END
				ELSE
				BEGIN
					EXEC sys.sp_executesql @SQLQuery
				END
				SET @Message = 'Table: ' + @DatabaseName + '.' + @SchemaName + '.' + @TableName + ' has been renamed to: ' + @DatabaseName + '.' + @SchemaName + '.' + @TableName + '_Deprecated'
				RAISERROR(@Message,0,1) WITH NOWAIT;
			END

			/* Insert the records into the declared deprecation table */
			INSERT INTO etl.DeprecationDeclared (AddressID, ID, IDType, IDName, MarkedForDeprecation)
			SELECT
			dt.AddressID,
			dt.ID,
			dt.IDType,
			dt.IDName,
			SYSDATETIMEOFFSET()
			FROM @DeprecationTable AS dt
			WHERE NOT EXISTS (
				SELECT 1/0
				FROM etl.DeprecationDeclared AS d
				WHERE d.AddressID = dt.AddressID
					AND d.ID = dt.ID
					AND d.IDType = dt.IDType
				)
			COMMIT TRANSACTION
		END TRY
		BEGIN CATCH
			SET @CatchErrorMessage = ERROR_MESSAGE() + N' Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(30)) + 
								N' Line: ' + CAST(ERROR_LINE() AS NVARCHAR(30)) + 
								N' Procedure: ' + ERROR_PROCEDURE()
			SET @ErrorSeverity = ERROR_SEVERITY()
			SET @ErrorState = ERROR_STATE()

			ROLLBACK;

			RAISERROR(@CatchErrorMessage, @ErrorSeverity, @ErrorState)

		END CATCH
	END
	ELSE
	BEGIN
		BEGIN TRY
			BEGIN TRANSACTION
			/* Activate the records again and load the lineage again */
			UPDATE t
			SET Active = 1,
				MuteTimelyCheck = 0,
				LoadLineage = 1
			OUTPUT @AddressID, deleted.PipelineID, 'PipelineID', deleted.PipelineName
			INTO @DeprecationTable
			FROM etl.Pipeline AS t
			INNER JOIN #Pipelines AS p
				ON p.PipelineID = t.PipelineID

			/* "un-deprecate" address record */
			UPDATE etl.Address
			SET Deprecated = 0
			WHERE AddressID = @AddressID
		
			/* Re-activate audit rules */
			UPDATE t
			SET RuleEndDate = '9999-12-31',
				Active = 1
			OUTPUT @AddressID, deleted.AuditColumnRuleID, 'AuditColumnRuleID', deleted.AuditColumn
			INTO @DeprecationTable
			FROM etl.AuditColumnRule AS t
			INNER JOIN etl.DeprecationDeclared AS d
				ON d.ID = t.AuditColumnRuleID
				AND d.IDType = 'AuditColumnRuleID'
				AND d.AddressID = @AddressID

			UPDATE t
			SET RuleEndDate = '9999-12-31',
				Active = 1
			OUTPUT @AddressID, deleted.AuditCustomRuleID, 'AuditCustomRuleID', NULL
			INTO @DeprecationTable
			FROM etl.AuditCustomRule AS t
			INNER JOIN etl.DeprecationDeclared AS d
				ON d.ID = t.AuditCustomRuleID
				AND d.IDType = 'AuditCustomRuleID'
				AND d.AddressID = @AddressID

			/* Rename from TableName_Deprecated back to TableName if flagged */
			IF @AddressType = 'Database' AND DB_NAME() = @DatabaseName AND @RenameTable = 1
			BEGIN
				SET @SQLQuery = 'EXEC sp_rename ''' + @SchemaName + '.' + @TableName + '_Deprecated' + ''', ''' + @TableName + ''';'
				IF @Debug = 1
				BEGIN
					PRINT @SQLQuery
				END
				ELSE
				BEGIN
					EXEC sys.sp_executesql @SQLQuery
				END
				SET @Message = 'Table: ' + @DatabaseName + '.' + @SchemaName + '.' + @TableName + '_Deprecated has been renamed back to: ' + @DatabaseName + '.' + @SchemaName + '.' + @TableName + ''
				RAISERROR(@Message,0,1) WITH NOWAIT;
			END

			/* Remove the IDs from the deprecation table since no longer declared */
			DELETE d 
			FROM etl.DeprecationDeclared AS d
			INNER JOIN @DeprecationTable AS dt
				ON dt.AddressID = d.AddressID
				AND dt.ID = d.ID
				AND dt.IDType = d.IDType
			COMMIT TRANSACTION
		END TRY 
		BEGIN CATCH
			SET @CatchErrorMessage = ERROR_MESSAGE() + N' Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(30)) + 
								N' Line: ' + CAST(ERROR_LINE() AS NVARCHAR(30)) + 
								N' Procedure: ' + ERROR_PROCEDURE()
			SET @ErrorSeverity = ERROR_SEVERITY()
			SET @ErrorState = ERROR_STATE()

			ROLLBACK;

			RAISERROR(@CatchErrorMessage, @ErrorSeverity, @ErrorState)

		END CATCH
	END

	IF @@TRANCOUNT > 0 
		ROLLBACK;

	DROP TABLE #Pipelines

	SELECT
	'etl.DeprecationDeclared' AS TableName,
	*
	FROM etl.DeprecationDeclared
	WHERE AddressID = @AddressID

END