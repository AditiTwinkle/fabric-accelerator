﻿-- ALTER TABLE [ELT].[IngestDefinition]
-- 	ADD CONSTRAINT [DC_IngestDefinition_ModifiedTimestamp]
-- 	DEFAULT CONVERT(datetime2,CONVERT(datetimeoffset, getdate()) at time zone 'AUS Eastern Standard Time')
-- 	FOR [ModifiedTimestamp]
