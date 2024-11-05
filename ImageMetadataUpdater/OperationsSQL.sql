--CREATE TABLE [dbo].[tblMetaUpdated](
--	[ImageID] [int] NOT NULL,
--	[Status] [int] NOT NULL,
--	[Reason] [text] NULL,
--	[Width] [int] NULL,
--	[Height] [int] NULL,
--	[Changed] [int] NULL,
--	[GifWidth] [int] NULL,
--	[GifHeight] [int] NULL
--) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
--GO

--delete from tblMetaUpdated

--insert into tblMetaUpdated (imageid, status, reason)
--select imageid, 0, '' from tblImages where 1=1
----and  companyID = 1
--and deleted <> 1

--update tblMetaUpdated set Status = 0, Reason = NULL,Changed = NULL, Width = null, Height = null where 1=1 and status < 0

--delete from tblMetaUpdated where Reason like '%does not exist.'
--delete from tblMetaUpdated where Status < 0

--select * from tblMetaUpdated where Status <> 2

select * from tblMetaUpdated where Status = 2 and changed <> 0