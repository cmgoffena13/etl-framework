--THROW 51000, 'Make sure to designate the right database for the address and then comment this out', 1;
/* Run these individually */

/* Stage 1 Deprecation - decision to deprecate the target address, no longer needed */
/* This stage does not involve code */

/* Stage 2 Deprecation - declare the target deprecated. Turn everything off. */
/* Noticed that the Pipelinees and rules are marked in-active */
EXEC etl.DeprecationDeclared_UID @FullAddress = 'ETL_Metadata.test.TargetPublish',
								 @Revert = 0,
								 @RenameTable = 1

/* Stage 3 Deprecation - Create tickets to remove physical objects after alerted */
/* Will be alerted by etl.DeprecationFinalCheck procedure after waiting period */
/* Create tickets and mark the deprecation records TicketsCreated = 1, mutes alert */
EXEC etl.DeprecationFinal_U @FullAddress = 'ETL_Metadata.test.TargetPublish'


/* Alongside removal of physical objects, data deletion will occur */
EXEC etl.DeprecationFinal_D @FullAddress = 'ETL_Metadata.test.TargetPublish'

/* Validation that everything has been removed */
SELECT * FROM etl.Pipeline
SELECT * FROM etl.RunTime
SELECT * FROM etl.AuditColumnRule
SELECT * FROM etl.AuditColumnLog
SELECT * FROM etl.AuditCustomRule
SELECT * FROM etl.AuditCustomLog

/* Notice that the target address has been dropped as well
SELECT * FROM test.TargetPublish
*/