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

update tblMetaUpdated set Status = 0, Reason = NULL,Changed = NULL, Width = null, Height = null where 1=1 -- and status < 0
 --and imageid in (
 ----2802411, 
 --2802415
 ----2802412
 --)



--delete from tblMetaUpdated where Reason like '%does not exist.'
--delete from tblMetaUpdated where Status < 0

select * from tblMetaUpdated where Status <> 2

--select * from tblMetaUpdated where Status = 2 and changed <> 0