﻿-- ALTER TABLE [ELT].[L1TransformDefinition]
-- 	ADD CONSTRAINT [DC_L1TransformDefinition_ModifiedTimestamp]
-- 	DEFAULT CONVERT(datetime2,CONVERT(datetimeoffset, getdate()) at time zone 'AUS Eastern Standard Time')
-- 	FOR [ModifiedTimestamp]
