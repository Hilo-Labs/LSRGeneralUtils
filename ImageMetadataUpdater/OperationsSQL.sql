--CREATE TABLE [dbo].[tblMetaUpdated](
--	[ImageID] [int] NOT NULL,
--	[Status] [int] NOT NULL,
--	[Reason] [text] NULL,
--	[Width] [int] NULL,
--	[Height] [int] NULL
--) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
--GO

--update tblMetaUpdated set Status = 0, Reason = NULL, Width = null, Height = null where status < 0

--delete from tblMetaUpdated where Reason like '%full.tif does not exist.'
--delete from tblMetaUpdated where Status < 0

select * from tblMetaUpdated
where Status <> 2