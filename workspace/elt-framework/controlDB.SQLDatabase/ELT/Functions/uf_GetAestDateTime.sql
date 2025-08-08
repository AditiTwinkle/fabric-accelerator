-- Function: ELT.uf_GetAestDateTime
-- Purpose: Returns the current date and time in Australian Eastern Standard Time (AEST).
-- It converts the current server date/time (GETDATE()) to the AEST timezone using SQL Server's AT TIME ZONE feature.
-- Returns: DATETIME value in AEST timezone.
CREATE FUNCTION ELT.[uf_GetAestDateTime]()
RETURNS DATETIME
WITH EXECUTE AS CALLER
AS
 BEGIN
    RETURN CONVERT(datetime,CONVERT(datetimeoffset, getdate()) AT TIME ZONE 'AUS Eastern Standard Time')
END

GO

