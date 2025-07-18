USE [analyticsroutinedev]
GO
/****** Object:  Table [dbo].[USERPROFILE_FIELD]    Script Date: 10/07/2025 09:32:58 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[USERPROFILE_FIELD](
	[USERPROFILE_FIELD] [int] NOT NULL,
	[FIELD_NAME] [nvarchar](max) NOT NULL,
	[SETTINGS] [xml] NULL,
	[RESTRICTIONS] [xml] NULL,
	[DELETED] [int] NULL,
	[TAB] [int] NOT NULL,
	[PURPOSE] [nvarchar](max) NULL,
	[GUIDELINES] [nvarchar](max) NULL,
	[IGNORE_FROM_AUDIT] [bit] NOT NULL,
	[REFERENCE_UUID] [uniqueidentifier] NULL,
	[STANDARD_REFERENCE_ID] [int] NULL,
 CONSTRAINT [PK_USERPROFILE_FIELD] PRIMARY KEY CLUSTERED 
(
	[USERPROFILE_FIELD] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
ALTER TABLE [dbo].[USERPROFILE_FIELD] ADD  DEFAULT ((1)) FOR [TAB]
GO
ALTER TABLE [dbo].[USERPROFILE_FIELD] ADD  CONSTRAINT [DF__USERPROFILE_FIELD__IGNORE_FROM_AUDIT]  DEFAULT ((0)) FOR [IGNORE_FROM_AUDIT]
GO
ALTER TABLE [dbo].[USERPROFILE_FIELD]  WITH CHECK ADD  CONSTRAINT [fk_USERPROFILE_FIELD_REFERENCE_UUID] FOREIGN KEY([REFERENCE_UUID])
REFERENCES [dbo].[USERPROFILE_STANDARD_DATA_REFERENCE] ([UUID])
GO
ALTER TABLE [dbo].[USERPROFILE_FIELD] CHECK CONSTRAINT [fk_USERPROFILE_FIELD_REFERENCE_UUID]
GO
ALTER TABLE [dbo].[USERPROFILE_FIELD]  WITH CHECK ADD  CONSTRAINT [FK_USERPROFILE_FIELD_STANDARD_REFERENCE_ID] FOREIGN KEY([STANDARD_REFERENCE_ID])
REFERENCES [dbo].[USERPROFILE_STANDARD_DATA_REFERENCE] ([REFERENCE_ID])
GO
ALTER TABLE [dbo].[USERPROFILE_FIELD] CHECK CONSTRAINT [FK_USERPROFILE_FIELD_STANDARD_REFERENCE_ID]
GO
