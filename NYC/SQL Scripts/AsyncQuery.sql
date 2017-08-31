USE TaxiData
GO


DECLARE @chunk INT =4

WHILE (1=1)
BEGIN
	
	DECLARE @query			VARCHAR(MAX),
			@RunningQuery	INT=0
	
	WAITFOR DELAY '00:00:01'
	
	SELECT TOP 1 @query = QUERY
	FROM   TripInsertStmnt AS tis
	WHERE  tis.StartTime IS NULL
	       AND tis.EndTime IS NULL
	ORDER BY
	       tis.ID
	
	IF (
	       SELECT COUNT(*)
	       FROM   TripInsertStmnt AS tis WITH (NOLOCK)
	       WHERE  tis.StartTime IS  NULL
	              AND tis.EndTime IS  NULL
	   ) = 0
	    BREAK;
	
	IF (
	       SELECT COUNT(*)
	       FROM   TripInsertStmnt AS tis WITH (NOLOCK)
	       WHERE  tis.StartTime IS NOT NULL
	              AND tis.EndTime IS NULL
	   ) < @chunk
	BEGIN
			EXEC ExecuteSQL_ByAgentJob_usp
				@SqlStatemet = @query,
				@SPNameOrStmntTitle = 'Job',
				@JobRunningUser = sa,
				@JobIdOut = null
			WAITFOR DELAY '00:00:01'
	END
	
	IF @query IS NULL BREAK;
	
END