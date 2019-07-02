USE [Maintenance]
GO
/****** Object:  UserDefinedFunction [dbo].[fn_Round5min]    Script Date: 7/1/2019 10:17:24 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[fn_Round5min]
(
@date datetime
) returns datetime
as
begin -- adding 150 seconds to round off instead of truncating
return dateadd(minute, datediff(minute, '1900-01-01', dateadd(second, 150, @date))/5*5, 0)
end

GO
/****** Object:  Table [dbo].[ApplicationSetting]    Script Date: 7/1/2019 10:17:24 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ApplicationSetting](
	[SettingCode] [varchar](100) NOT NULL,
	[Description] [varchar](250) NULL,
	[SettingValue] [varchar](1000) NOT NULL,
	[SettingGroupCode] [varchar](10) NOT NULL,
	[DateCreated] [datetime] NOT NULL,
	[DateLastUpdated] [datetime] NOT NULL,
	[DataType] [varchar](25) NOT NULL,
 CONSTRAINT [PK_ApplicationSetting] PRIMARY KEY CLUSTERED 
(
	[SettingCode] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[blocking_spid]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[blocking_spid](
	[spid] [smallint] NOT NULL,
	[blocked] [smallint] NOT NULL,
	[waittime] [bigint] NOT NULL,
	[cmd] [nchar](16) NOT NULL,
	[hostname] [nchar](128) NOT NULL,
	[program_name] [nchar](128) NOT NULL,
	[loginame] [nchar](128) NOT NULL,
	[block_datetime] [datetime] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CounterData]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CounterData](
	[GUID] [uniqueidentifier] NOT NULL,
	[CounterID] [int] NOT NULL,
	[RecordIndex] [int] NOT NULL,
	[CounterDateTime] [char](24) NOT NULL,
	[CounterValue] [float] NOT NULL,
	[FirstValueA] [int] NULL,
	[FirstValueB] [int] NULL,
	[SecondValueA] [int] NULL,
	[SecondValueB] [int] NULL,
	[MultiCount] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[GUID] ASC,
	[CounterID] ASC,
	[RecordIndex] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CounterDetails]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CounterDetails](
	[CounterID] [int] IDENTITY(1,1) NOT NULL,
	[MachineName] [varchar](1024) NOT NULL,
	[ObjectName] [varchar](1024) NOT NULL,
	[CounterName] [varchar](1024) NOT NULL,
	[CounterType] [int] NOT NULL,
	[DefaultScale] [int] NOT NULL,
	[InstanceName] [varchar](1024) NULL,
	[InstanceIndex] [int] NULL,
	[ParentName] [varchar](1024) NULL,
	[ParentObjectID] [int] NULL,
	[TimeBaseA] [int] NULL,
	[TimeBaseB] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[CounterID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CTApplication]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CTApplication](
	[ApplicationID] [int] NOT NULL,
	[ApplicationName] [nvarchar](128) NOT NULL,
 CONSTRAINT [PK_CTApplication] PRIMARY KEY CLUSTERED 
(
	[ApplicationID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CTConfig]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CTConfig](
	[OptionCode] [nvarchar](20) NOT NULL,
	[OptionValue] [sql_variant] NOT NULL,
 CONSTRAINT [PK_CTConfig] PRIMARY KEY CLUSTERED 
(
	[OptionCode] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CTDateDimension]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CTDateDimension](
	[DateID] [int] IDENTITY(1,1) NOT NULL,
	[CalendarDate] [datetime] NOT NULL,
	[DayOfWeekNum] [tinyint] NOT NULL,
	[DayOfWeekName] [varchar](20) NOT NULL,
	[DayOfMonthNum] [tinyint] NOT NULL,
	[DayOfYearNum] [smallint] NOT NULL,
	[WeekInYearNum] [tinyint] NOT NULL,
	[MonthInYearNum] [tinyint] NOT NULL,
	[MonthName] [varchar](20) NOT NULL,
 CONSTRAINT [PK_CTDateDimension] PRIMARY KEY CLUSTERED 
(
	[DateID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CTHost]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CTHost](
	[HostID] [int] NOT NULL,
	[HostName] [nvarchar](128) NOT NULL,
 CONSTRAINT [PK_CTHost] PRIMARY KEY CLUSTERED 
(
	[HostID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CTLogin]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CTLogin](
	[LoginID] [int] NOT NULL,
	[LoginName] [nvarchar](256) NOT NULL,
 CONSTRAINT [PK_CTLogin] PRIMARY KEY CLUSTERED 
(
	[LoginID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CTServer]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CTServer](
	[ServerID] [int] NOT NULL,
	[ServerName] [nvarchar](256) NOT NULL,
 CONSTRAINT [PK_CTServer] PRIMARY KEY CLUSTERED 
(
	[ServerID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CTTextData]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CTTextData](
	[TextDataHashCode] [bigint] NOT NULL,
	[NormalizedTextData] [nvarchar](4000) NOT NULL,
	[SampleTextData] [ntext] NOT NULL,
 CONSTRAINT [PK_CTTextData] PRIMARY KEY CLUSTERED 
(
	[TextDataHashCode] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CTTrace]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CTTrace](
	[TraceID] [int] NOT NULL,
	[TraceName] [nvarchar](128) NOT NULL,
 CONSTRAINT [PK_CTTrace] PRIMARY KEY CLUSTERED 
(
	[TraceID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
 CONSTRAINT [IX_CTTrace_Unique] UNIQUE NONCLUSTERED 
(
	[TraceName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CTTraceDetail]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CTTraceDetail](
	[RowNumber] [int] IDENTITY(1,1) NOT NULL,
	[TraceID] [int] NOT NULL,
	[EventClass] [smallint] NOT NULL,
	[TextDataHashCode] [bigint] NULL,
	[CPU] [int] NULL,
	[Reads] [bigint] NULL,
	[Writes] [bigint] NULL,
	[Duration] [bigint] NULL,
	[ApplicationID] [int] NOT NULL,
	[LoginID] [int] NOT NULL,
	[HostID] [int] NOT NULL,
	[EndTime] [datetime] NOT NULL,
	[TraceFileID] [int] NOT NULL,
	[ServerID] [int] NOT NULL,
 CONSTRAINT [PK_CTTraceDetail] PRIMARY KEY CLUSTERED 
(
	[RowNumber] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CTTraceFile]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CTTraceFile](
	[TraceFileID] [int] NOT NULL,
	[TraceFileName] [nvarchar](512) NOT NULL,
PRIMARY KEY CLUSTERED 
(
	[TraceFileID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[CTTraceSummary]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[CTTraceSummary](
	[RowID] [int] IDENTITY(1,1) NOT NULL,
	[TraceID] [int] NOT NULL,
	[TraceFileID] [int] NOT NULL,
	[DateID] [int] NOT NULL,
	[TimeID] [int] NOT NULL,
	[ServerID] [int] NULL,
	[EventClass] [smallint] NOT NULL,
	[ApplicationID] [int] NOT NULL,
	[LoginID] [int] NOT NULL,
	[HostID] [int] NOT NULL,
	[TextDataHashCode] [bigint] NULL,
	[CPU] [int] NULL,
	[Reads] [bigint] NULL,
	[Writes] [bigint] NULL,
	[Duration] [bigint] NULL,
	[ExecutionCount] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[RowID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[DiskFreeSpace]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DiskFreeSpace](
	[DriveLetter] [char](1) NOT NULL,
	[FreeMB] [int] NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[DiskFreeSpace_All]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DiskFreeSpace_All](
	[SQLServerName] [nvarchar](100) NULL,
	[SurveyDateTime] [datetime] NULL,
	[DriveLetter] [char](1) NOT NULL,
	[FreeMB] [int] NOT NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[DisplayToID]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[DisplayToID](
	[GUID] [uniqueidentifier] NOT NULL,
	[RunID] [int] NULL,
	[DisplayString] [varchar](1024) NOT NULL,
	[LogStartTime] [char](24) NULL,
	[LogStopTime] [char](24) NULL,
	[NumberOfRecords] [int] NULL,
	[MinutesToUTC] [int] NULL,
	[TimeZoneName] [char](32) NULL,
PRIMARY KEY CLUSTERED 
(
	[GUID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY],
UNIQUE NONCLUSTERED 
(
	[DisplayString] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[LS_RestoreStatus]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[LS_RestoreStatus](
	[ServerName] [varchar](100) NULL,
	[sec_db] [varchar](100) NULL,
	[lrd] [datetime] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[mntblSQLExceptionLog_Detail]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[mntblSQLExceptionLog_Detail](
	[fReportID] [int] NOT NULL,
	[ItemID] [bigint] IDENTITY(1,1) NOT NULL,
	[ErrorTime] [datetime] NOT NULL,
	[ServerName] [nvarchar](100) NOT NULL,
	[ClientHostName] [nvarchar](100) NULL,
	[DatabaseID] [int] NULL,
	[UserName] [nvarchar](100) NULL,
	[ErrorSeverity] [bigint] NULL,
	[ErrorNumber] [bigint] NULL,
	[Error] [nvarchar](512) NULL,
	[ErrorMessage] [nvarchar](512) NULL,
	[SQLText] [nvarchar](max) NULL,
	[EventData] [xml] NULL,
 CONSTRAINT [PK_mntblSQLExceptionLog_Detail] PRIMARY KEY CLUSTERED 
(
	[fReportID] ASC,
	[ItemID] ASC,
	[ErrorTime] ASC,
	[ServerName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[mntblSQLExceptionLog_Header]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[mntblSQLExceptionLog_Header](
	[fReportID] [bigint] IDENTITY(1,1) NOT NULL,
	[fEnvironment] [varchar](20) NULL,
	[fCreated] [datetime] NULL,
	[fDateTimeBeg] [datetime] NULL,
	[fDatetimeEnd] [datetime] NULL,
 CONSTRAINT [PK_mntblSQLExceptionLog_Header] PRIMARY KEY CLUSTERED 
(
	[fReportID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[mnttblArchivedTables]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[mnttblArchivedTables](
	[fID] [bigint] IDENTITY(1,1) NOT NULL,
	[fWritten] [datetime] NULL,
	[fDatabaseName] [varchar](100) NOT NULL,
	[fSchema] [varchar](100) NOT NULL,
	[fTableName] [varchar](100) NOT NULL,
	[fStagingTable] [varchar](200) NOT NULL,
	[fArchivedTable] [varchar](200) NOT NULL,
	[fDateBegin] [datetime] NOT NULL,
	[fDateEnd] [datetime] NOT NULL,
	[fRetentionDays] [int] NOT NULL,
	[fRetentionDays_ColumnName] [varchar](100) NOT NULL,
	[fArchived_RecordLimit] [int] NOT NULL,
	[fMoveToArchive] [bit] NOT NULL,
	[fMovedToArchive] [bit] NOT NULL,
	[fPrimaryKey_Number] [int] NOT NULL,
	[fPrimaryKey_1] [varchar](100) NOT NULL,
	[fPrimaryKey_2] [varchar](100) NOT NULL,
	[fPrimaryKey_3] [varchar](100) NOT NULL,
	[fPrimaryKey_4] [varchar](100) NOT NULL,
	[fArchivedRecordsCnt] [int] NOT NULL,
	[fDisabled] [bit] NOT NULL,
	[fDelete] [bit] NOT NULL,
	[fTime] [datetime] NOT NULL,
	[fModified] [datetime] NOT NULL,
	[fModifiedID] [int] NOT NULL,
	[fCreated] [datetime] NOT NULL,
	[fCreatedID] [int] NOT NULL,
	[fQueueID] [int] NOT NULL,
 CONSTRAINT [PK_mnttblArchivedTables] PRIMARY KEY CLUSTERED 
(
	[fID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[mnttblArchivedTables_RoutineErrors]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[mnttblArchivedTables_RoutineErrors](
	[fID] [bigint] IDENTITY(1,1) NOT NULL,
	[fWritten] [datetime] NULL,
	[fErrorNumber] [int] NULL,
	[fErrorSeverity] [int] NULL,
	[fErrorState] [int] NULL,
	[fErrorLine] [int] NULL,
	[fErrorProcedure] [nvarchar](128) NULL,
	[fErrorMessage] [nvarchar](4000) NULL,
 CONSTRAINT [PK_mnttblArchivedTables_RoutineErrors] PRIMARY KEY CLUSTERED 
(
	[fID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[mnttblArchivedTables_RunningQueues]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[mnttblArchivedTables_RunningQueues](
	[fTableName] [varchar](300) NOT NULL,
	[fQueueID] [int] NOT NULL,
	[fTime] [datetime] NOT NULL,
 CONSTRAINT [PK_mnttblArchivedTables_RunningQueues] PRIMARY KEY CLUSTERED 
(
	[fTableName] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 80) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[mnttblBackupInfo]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[mnttblBackupInfo](
	[fBIID] [int] IDENTITY(1,1) NOT NULL,
	[fSQLServerName] [nvarchar](50) NULL,
	[fDatabaseName] [nvarchar](100) NULL,
	[fLastFullBackup] [datetime] NULL,
	[fLastLogBackup] [datetime] NULL,
	[fDBCreateDate] [datetime] NULL,
	[fRecoveryModel] [nvarchar](35) NULL,
	[fAlertFlag] [bit] NULL,
	[fCompatibilityLevel] [int] NULL,
	[fDbSizeMb] [decimal](18, 2) NULL,
	[fLogSizeMb] [decimal](9, 2) NULL,
	[fLogSizePct] [decimal](9, 2) NULL,
PRIMARY KEY CLUSTERED 
(
	[fBIID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[mnttblCounterDetails_All]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[mnttblCounterDetails_All](
	[ServerName] [varchar](100) NOT NULL,
	[CounterID] [int] NOT NULL,
	[CounterName] [varchar](1024) NULL,
	[CounterDateTime] [datetime] NOT NULL,
	[CounterValue] [float] NULL,
 CONSTRAINT [PK_mnttblCounterDetails_All] PRIMARY KEY CLUSTERED 
(
	[ServerName] ASC,
	[CounterID] ASC,
	[CounterDateTime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[mnttblCounterDetails_All_Archive]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[mnttblCounterDetails_All_Archive](
	[ServerName] [varchar](100) NOT NULL,
	[CounterID] [int] NOT NULL,
	[CounterName] [varchar](1024) NULL,
	[CounterDateTime] [datetime] NOT NULL,
	[CounterValue] [float] NULL,
 CONSTRAINT [PK_mnttblCounterDetails_All_Archive] PRIMARY KEY CLUSTERED 
(
	[ServerName] ASC,
	[CounterID] ASC,
	[CounterDateTime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[mnttblCounterDetails_Avg]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[mnttblCounterDetails_Avg](
	[CounterAvgID] [int] IDENTITY(1,1) NOT NULL,
	[ServerName] [varchar](100) NOT NULL,
	[CounterName] [varchar](1024) NULL,
	[CounterDate] [datetime] NOT NULL,
	[CounterTimeRange] [varchar](55) NULL,
	[CounterValueAvg] [decimal](18, 2) NULL,
PRIMARY KEY CLUSTERED 
(
	[CounterAvgID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[mnttblExceptionLog_Summary]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[mnttblExceptionLog_Summary](
	[LogID] [int] IDENTITY(1,1) NOT NULL,
	[SQLServerName] [nvarchar](100) NULL,
	[ErrorDay] [date] NULL,
	[ErrorCount] [int] NULL,
PRIMARY KEY CLUSTERED 
(
	[LogID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[mnttblPerfMonResultsAll]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[mnttblPerfMonResultsAll](
	[fID] [int] IDENTITY(1,1) NOT NULL,
	[fServerName] [varchar](35) NULL,
	[fTime] [datetime] NULL,
	[fMemoryPagesSec] [decimal](18, 9) NULL,
	[fProcessorTime] [decimal](18, 9) NULL,
	[fBufferCache] [decimal](18, 9) NULL,
	[fTransactionsSec] [decimal](18, 9) NULL,
	[fUserConnections] [decimal](18, 9) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[mnttblPerfMonResultsAll_bak]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[mnttblPerfMonResultsAll_bak](
	[fServerName] [varchar](35) NULL,
	[fTime] [datetime] NULL,
	[fMemoryPagesSec] [decimal](18, 9) NULL,
	[fProcessorTime] [decimal](18, 9) NULL,
	[fBufferCache] [decimal](18, 9) NULL,
	[fTransactionsSec] [decimal](18, 9) NULL,
	[fUserConnections] [decimal](18, 9) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[mnttblSQLServerClientConns]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[mnttblSQLServerClientConns](
	[ClientConnsId] [int] IDENTITY(1,1) NOT NULL,
	[ServerName] [nvarchar](128) NULL,
	[ConnCount] [int] NULL,
	[hostname] [nvarchar](128) NULL,
	[program_name] [nvarchar](128) NULL,
	[loginame] [nvarchar](128) NULL,
	[DBName] [nvarchar](128) NULL,
	[ConnDateHour] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[ClientConnsId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[mnttblSQLServerClientConns_ALL]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[mnttblSQLServerClientConns_ALL](
	[ClientConnsId] [int] IDENTITY(1,1) NOT NULL,
	[ServerName] [nvarchar](128) NULL,
	[ConnCount] [int] NULL,
	[hostname] [nvarchar](128) NULL,
	[program_name] [nvarchar](128) NULL,
	[loginame] [nvarchar](128) NULL,
	[DBName] [nvarchar](128) NULL,
	[ConnDateHour] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[ClientConnsId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[mnttblSQLServerClientConnsAll_staging]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[mnttblSQLServerClientConnsAll_staging](
	[ClientConnsId] [int] IDENTITY(1,1) NOT NULL,
	[ServerName] [nvarchar](128) NULL,
	[ConnCount] [int] NULL,
	[hostname] [nvarchar](128) NULL,
	[program_name] [nvarchar](128) NULL,
	[loginame] [nvarchar](128) NULL,
	[DBName] [nvarchar](128) NULL,
	[ConnDateHour] [datetime] NULL,
PRIMARY KEY CLUSTERED 
(
	[ClientConnsId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[montblXE_CPUReadsFilterTrace_Consol]    Script Date: 7/1/2019 10:17:25 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[montblXE_CPUReadsFilterTrace_Consol](
	[ConsolId] [bigint] IDENTITY(1,1) NOT NULL,
	[XEID] [int] NOT NULL,
	[SqlServerName] [nvarchar](100) NOT NULL,
	[timestamp] [datetime] NULL,
	[pdtlocaltime] [datetime] NULL,
	[groupby_val] [nvarchar](500) NULL,
	[item_count] [int] NULL,
	[event_name] [nvarchar](200) NULL,
	[cpu_time] [decimal](20, 0) NULL,
	[duration] [decimal](20, 0) NULL,
	[physical_reads] [decimal](20, 0) NULL,
	[logical_reads] [decimal](20, 0) NULL,
	[writes] [decimal](20, 0) NULL,
	[object_name] [nvarchar](200) NULL,
	[statement] [nvarchar](max) NULL,
	[username] [nvarchar](200) NULL,
	[session_id] [int] NULL,
	[query_hash] [decimal](20, 0) NULL,
	[nt_username] [nvarchar](200) NULL,
	[database_name] [nvarchar](200) NULL,
	[database_id] [int] NULL,
	[client_hostname] [nvarchar](200) NULL,
	[sql_text] [nvarchar](max) NULL,
	[is_processed] [bit] NULL,
PRIMARY KEY CLUSTERED 
(
	[ConsolId] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
/****** Object:  Table [dbo].[ServerDiskCap]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[ServerDiskCap](
	[SQLServerName] [nvarchar](100) NULL,
	[DriveLetter] [char](1) NULL,
	[DriveCapacityMB] [int] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[SQLErrorLog]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[SQLErrorLog](
	[PKID] [int] IDENTITY(1,1) NOT NULL,
	[Server] [nvarchar](128) NOT NULL,
	[LogEntryDate] [datetime] NULL,
	[SPID] [varchar](50) NULL,
	[LogEntry] [nvarchar](4000) NULL,
	[ID] [int] NULL,
	[IsChanged] [bit] NULL,
 CONSTRAINT [PK_SQLErrorLog] PRIMARY KEY CLUSTERED 
(
	[PKID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[SQLErrorLogs]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[SQLErrorLogs](
	[PKID] [int] NOT NULL,
	[Server] [nvarchar](128) NOT NULL,
	[LogEntryDate] [datetime] NULL,
	[SPID] [varchar](50) NULL,
	[LogEntry] [nvarchar](4000) NULL,
	[ID] [int] NULL,
	[IsChanged] [bit] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[SSIS_SQLServer]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[SSIS_SQLServer](
	[SQLServerID] [int] IDENTITY(1,1) NOT NULL,
	[SQLServerName] [nvarchar](50) NULL,
	[Edition] [nvarchar](128) NULL,
	[LicenseType] [nvarchar](128) NULL,
	[NumLicenses] [int] NULL,
	[ProductVersion] [nvarchar](128) NULL,
	[ProductLevel] [nvarchar](128) NULL,
	[SqlCharSetName] [nvarchar](128) NULL,
	[SqlSortOrderName] [nvarchar](128) NULL,
	[Collation] [nvarchar](128) NULL,
	[OSVersion] [nvarchar](55) NULL,
	[SQLServerType] [nvarchar](255) NULL,
	[IsMonitor] [bit] NULL,
	[Comments] [varchar](200) NULL,
 CONSTRAINT [PK__SSIS_SQLServer__4F7CD00D] PRIMARY KEY CLUSTERED 
(
	[SQLServerID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[SSISConfigParams]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[SSISConfigParams](
	[ItemID] [int] IDENTITY(1,1) NOT NULL,
	[SSISParamName] [varchar](255) NULL,
	[SSISParamValue] [datetime] NULL,
	[ServerName] [varchar](100) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[stg_mnttblCounterDetails_All_Archive]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[stg_mnttblCounterDetails_All_Archive](
	[ServerName] [varchar](100) NOT NULL,
	[CounterID] [int] NOT NULL,
	[CounterDateTime] [datetime] NOT NULL,
	[fProcessed] [bit] NULL,
 CONSTRAINT [PK_stg_mnttblCounterDetails_All_Archive] PRIMARY KEY CLUSTERED 
(
	[ServerName] ASC,
	[CounterID] ASC,
	[CounterDateTime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[tblBlockingSpids]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tblBlockingSpids](
	[spid] [int] NULL,
	[cmd] [nvarchar](500) NULL,
	[hostname] [nvarchar](100) NULL,
	[program_name] [nvarchar](500) NULL,
	[loginame] [nvarchar](500) NULL,
	[timelogged] [datetime] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[tblTableDefragMaint]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tblTableDefragMaint](
	[fID] [int] IDENTITY(1,1) NOT NULL,
	[fServerName] [varchar](35) NULL,
	[fDatabaseName] [varchar](25) NULL,
	[fTableID] [int] NULL,
	[fTableName] [varchar](255) NULL,
	[fIndexName] [varchar](255) NULL,
	[fFragPercent] [int] NULL,
	[fJobStartDate] [datetime] NULL,
	[fDefragStartTime] [datetime] NULL,
	[fDefragEndTime] [datetime] NULL,
	[fUpdateStatsStartTime] [datetime] NULL,
	[fUpdateStatsEndTime] [datetime] NULL,
 CONSTRAINT [PK_tblTableDefragMaint] PRIMARY KEY CLUSTERED 
(
	[fID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[TFS_PerfMonResults]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[TFS_PerfMonResults](
	["(PDH-CSV 4 0) (Mountain Standard Time)(420)"] [varchar](50) NULL,
	["  TFS Memory Pages sec"] [varchar](50) NULL,
	["  TFS Processor(_Total) % Processor Time"] [varchar](50) NULL,
	["  TFS SQLServer Buffer Manager Buffer cache hit ratio"] [varchar](50) NULL,
	["  TFS SQLServer Databases(_Total) Transactions sec"] [varchar](50) NULL,
	["  TFS SQLServer General Statistics User Connections"] [varchar](50) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[tmpOut]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tmpOut](
	[ExecTime] [datetime] NULL,
	[ServerName] [varchar](255) NULL,
	[CounterDAte] [datetime] NULL,
	[AvgCPU] [float] NULL,
	[AvgBCHR] [float] NULL,
	[AvgPLE] [float] NULL,
	[AvgPage] [float] NULL,
	[AvgTran] [float] NULL,
	[AvgUserConn] [float] NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[tmpReplTables]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[tmpReplTables](
	[Publication] [varchar](30) NULL,
	[Subscriber] [varchar](30) NULL,
	[TableName] [varchar](100) NULL
) ON [PRIMARY]
GO
/****** Object:  Table [dbo].[WinEventLogs]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[WinEventLogs](
	[ComputerName] [nvarchar](255) NULL,
	[LogFile] [nvarchar](255) NULL,
	[Type] [nvarchar](255) NULL,
	[Message] [nvarchar](4000) NULL,
	[SourceName] [nvarchar](255) NULL,
	[TimeGenerated] [nvarchar](255) NULL,
	[TimeWritten] [nvarchar](255) NULL,
	[GenDateTime] [datetime] NULL
) ON [PRIMARY]
GO
/****** Object:  View [dbo].[CTTraceDetailView]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[CTTraceDetailView]
AS
SELECT 
	TD.[RowNumber],
	TD.[EventClass],
	TD.[TextDataHashCode],
	T.[NormalizedTextData],
	T.[SampleTextData],
	TD.[CPU],
	TD.[Reads],
	TD.[Writes],
	TD.[Duration],
	TD.[ApplicationID],
	A.[ApplicationName],
	TD.[LoginID],
	L.[LoginName],
	TD.[HostID],
	H.[HostName],
	TD.[EndTime],
	TD.[ServerID],
	S.[ServerName],
	TD.[TraceFileID],
	TD.TraceID,
	Tr.TraceName
FROM 
	[dbo].[CTTraceDetail] TD
LEFT JOIN
	[dbo].[CTApplication] A ON A.ApplicationID = TD.ApplicationID
LEFT JOIN
	[dbo].[CTLogin] L ON L.LoginID = TD.LoginID
LEFT JOIN
	[dbo].[CTHost] H ON H.HostID = TD.[HostID]
LEFT JOIN
	[dbo].[CTTextData] T ON T.[TextDataHashCode] = TD.[TextDataHashCode]
LEFT JOIN
	[dbo].[CTServer] S ON S.[ServerID] = TD.[ServerID]
LEFT JOIN
	dbo.CTTrace Tr ON Tr.TraceID = TD.TraceID
GO
/****** Object:  View [dbo].[CTTraceSummaryView]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [dbo].[CTTraceSummaryView]
AS
SELECT 
	TD.[EventClass],
	TD.[TextDataHashCode],
	T.[NormalizedTextData],
	T.[SampleTextData],
	TD.[CPU],
	TD.[Reads],
	TD.[Writes],
	TD.[Duration],
	TD.[ExecutionCount],
	TD.[ApplicationID],
	A.[ApplicationName],
	TD.[LoginID],
	L.[LoginName],
	TD.[HostID],
	H.[HostName],   
	TD.[ServerID],
	S.[ServerName],
	TD.[TraceFileID],
	TD.TraceID,
	Tr.TraceName,
	TD.DateID,
	DD.CalendarDate,
	TD.TimeID
FROM 
	[dbo].[CTTraceSummary] TD
LEFT JOIN
	[dbo].[CTApplication] A ON A.ApplicationID = TD.ApplicationID
LEFT JOIN
	[dbo].[CTLogin] L ON L.LoginID = TD.LoginID
LEFT JOIN
	[dbo].[CTHost] H ON H.HostID = TD.[HostID]
LEFT JOIN
	[dbo].[CTTextData] T ON T.[TextDataHashCode] = TD.[TextDataHashCode]
LEFT JOIN
	[dbo].[CTServer] S ON S.[ServerID] = TD.[ServerID]
LEFT JOIN
	dbo.CTTrace Tr ON Tr.TraceID = TD.TraceID
LEFT JOIN
	dbo.CTDateDimension DD ON DD.DateID = TD.DateID
GO
/****** Object:  Index [IX_CTTraceSummary_Application]    Script Date: 7/1/2019 10:17:26 PM ******/
CREATE NONCLUSTERED INDEX [IX_CTTraceSummary_Application] ON [dbo].[CTTraceSummary]
(
	[TraceID] ASC,
	[ApplicationID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
/****** Object:  Index [IX_CTTraceSummary_Date]    Script Date: 7/1/2019 10:17:26 PM ******/
CREATE NONCLUSTERED INDEX [IX_CTTraceSummary_Date] ON [dbo].[CTTraceSummary]
(
	[TraceID] ASC,
	[DateID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
/****** Object:  Index [IX_CTTraceSummary_Host]    Script Date: 7/1/2019 10:17:26 PM ******/
CREATE NONCLUSTERED INDEX [IX_CTTraceSummary_Host] ON [dbo].[CTTraceSummary]
(
	[TraceID] ASC,
	[HostID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
/****** Object:  Index [IX_CTTraceSummary_Login]    Script Date: 7/1/2019 10:17:26 PM ******/
CREATE NONCLUSTERED INDEX [IX_CTTraceSummary_Login] ON [dbo].[CTTraceSummary]
(
	[TraceID] ASC,
	[LoginID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
/****** Object:  Index [IX_CTTraceSummary_TraceFile]    Script Date: 7/1/2019 10:17:26 PM ******/
CREATE NONCLUSTERED INDEX [IX_CTTraceSummary_TraceFile] ON [dbo].[CTTraceSummary]
(
	[TraceID] ASC,
	[TraceFileID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
/****** Object:  Index [IX_mntblSQLExceptionLog_Detail_ErrorTime]    Script Date: 7/1/2019 10:17:26 PM ******/
CREATE NONCLUSTERED INDEX [IX_mntblSQLExceptionLog_Detail_ErrorTime] ON [dbo].[mntblSQLExceptionLog_Detail]
(
	[ErrorTime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
/****** Object:  Index [IX_CounterDatetime]    Script Date: 7/1/2019 10:17:26 PM ******/
CREATE NONCLUSTERED INDEX [IX_CounterDatetime] ON [dbo].[mnttblCounterDetails_All]
(
	[CounterDateTime] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
/****** Object:  Index [IX_LogEntryDate]    Script Date: 7/1/2019 10:17:26 PM ******/
CREATE NONCLUSTERED INDEX [IX_LogEntryDate] ON [dbo].[SQLErrorLogs]
(
	[LogEntryDate] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
SET ANSI_PADDING ON
GO
/****** Object:  Index [IX_WinEventLogs_GenDateTime]    Script Date: 7/1/2019 10:17:26 PM ******/
CREATE NONCLUSTERED INDEX [IX_WinEventLogs_GenDateTime] ON [dbo].[WinEventLogs]
(
	[GenDateTime] ASC
)
INCLUDE ( 	[ComputerName],
	[LogFile],
	[Type],
	[SourceName]) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
GO
ALTER TABLE [dbo].[CTTraceSummary] ADD  DEFAULT ((0)) FOR [TimeID]
GO
ALTER TABLE [dbo].[mnttblArchivedTables] ADD  CONSTRAINT [DF_mnttblArchivedTables_fWritten]  DEFAULT (getdate()) FOR [fWritten]
GO
ALTER TABLE [dbo].[mnttblArchivedTables] ADD  CONSTRAINT [DF_mnttblArchivedTables_fPrimaryKey_2]  DEFAULT ('') FOR [fPrimaryKey_2]
GO
ALTER TABLE [dbo].[mnttblArchivedTables] ADD  CONSTRAINT [DF_mnttblArchivedTables_fPrimaryKey_3]  DEFAULT ('') FOR [fPrimaryKey_3]
GO
ALTER TABLE [dbo].[mnttblArchivedTables] ADD  CONSTRAINT [DF_mnttblArchivedTables_fPrimaryKey_4]  DEFAULT ('') FOR [fPrimaryKey_4]
GO
ALTER TABLE [dbo].[mnttblArchivedTables] ADD  CONSTRAINT [DF_mnttblArchivedTables_fDisabled]  DEFAULT ((0)) FOR [fDisabled]
GO
ALTER TABLE [dbo].[mnttblArchivedTables] ADD  CONSTRAINT [DF_mnttblArchivedTables_fDelete]  DEFAULT ((0)) FOR [fDelete]
GO
ALTER TABLE [dbo].[mnttblArchivedTables] ADD  CONSTRAINT [DF_mnttblArchivedTables_fTime]  DEFAULT (getdate()) FOR [fTime]
GO
ALTER TABLE [dbo].[mnttblArchivedTables] ADD  CONSTRAINT [DF_mnttblArchivedTables_fModified]  DEFAULT (getdate()) FOR [fModified]
GO
ALTER TABLE [dbo].[mnttblArchivedTables] ADD  CONSTRAINT [DF_mnttblArchivedTables_fCreated]  DEFAULT (getdate()) FOR [fCreated]
GO
ALTER TABLE [dbo].[mnttblArchivedTables] ADD  CONSTRAINT [DF_mnttblArchivedTables_fQueueID]  DEFAULT ((1)) FOR [fQueueID]
GO
ALTER TABLE [dbo].[mnttblArchivedTables_RoutineErrors] ADD  CONSTRAINT [DF_mnttblArchivedTables_RoutineErrors_fWritten]  DEFAULT (getdate()) FOR [fWritten]
GO
ALTER TABLE [dbo].[mnttblArchivedTables_RunningQueues] ADD  CONSTRAINT [DF_mnttblArchivedTables_RunningQueues_fTime]  DEFAULT (getdate()) FOR [fTime]
GO
/****** Object:  StoredProcedure [dbo].[GetProcessCmd]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [dbo].[GetProcessCmd]

  @spid int

AS

SET NOCOUNT ON

DECLARE @SQL VARCHAR(100)

SET @SQL = 'dbcc inputbuffer(' + CAST(@spid AS VARCHAR(100)) + ')'
EXEC(@SQL)
GO
/****** Object:  StoredProcedure [dbo].[GetProcessCmd_Out]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [dbo].[GetProcessCmd_Out]

  @spid_in int

AS


SET NOCOUNT ON
SET ANSI_WARNINGS ON

CREATE TABLE #TEMP (EventType varchar(100), Parameters varchar(100), EventInfo varchar(3000))

INSERT INTO #TEMP (EventType, parameters, EventInfo)
EXEC GetProcessCmd @spid = @spid_in

SELECT 'EventType: ' + ISNULL(EventType, '') + ' Parameters: ' + ISNULL(Parameters, '') + ' EventInfo: ' + ISNULL(EventInfo, '') As fProcess
FROM #TEMP

DROP TABLE #TEMP
GO
/****** Object:  StoredProcedure [dbo].[mnt_sp_ArchiveTables]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE   PROCEDURE [dbo].[mnt_sp_ArchiveTables] ( @vQueueID INT )
AS
BEGIN

IF NOT EXISTS(SELECT TOP(1) fID FROM [Maintenance].[dbo].[mnttblArchivedTables] with (nolock)
				WHERE fDisabled = 0
				  and fDelete = 0
				  and fMoveToArchive in (1)
				  and fMovedToArchive = 0)
	BEGIN
		PRINT ' -- Nothing to Process'
		GOTO end_proc
	END
	

IF NOT EXISTS(select top(1) fQueueID FROM [Maintenance].[dbo].[mnttblArchivedTables] with (nolock) WHERE fQueueID = @vQueueID )
	BEGIN
		INSERT INTO [Maintenance].[dbo].[mnttblArchivedTables_RoutineErrors] ([fWritten],[fErrorNumber],[fErrorSeverity],[fErrorState],[fErrorLine],[fErrorProcedure],[fErrorMessage])
			 VALUES (GETDATE(), 5100, 16, 1, 0, '[Maintenance].[dbo].[mnttblArchivedTables]', 'ERROR: Invalid QueueID')
		RAISERROR ('ERROR: Invalid QueueID', 16, 1)
		GOTO end_proc
	END

SET NOCOUNT ON

declare @vDateBegin datetime, @vDateEnd datetime, @vNow datetime, @vRetentionDays INT, @vRetetionDays_ColumnName varchar(150) 
declare @vArchive_recLimit int, @vPrimaryKey_1 varchar(150), @vPrimaryKey_2 varchar(150), @vPrimaryKey_3 varchar(150)
declare @vPrimaryKey_4 varchar(150), @vArchivedRecordsCnt INT, @RecordsCnt INT, @vfID INT, @vfDatabaseName VARCHAR(150), @vfSchema VARCHAR(150)
declare @vTableName varchar(150), @vfArchivedTable VARCHAR(150), @vfStagingTable VARCHAR(150), @TotalPKs int, @vSourceTable VARCHAR(300)
declare @cmd_tmp_ins NVARCHAR(MAX), @cmd_tmp_upd_dups NVARCHAR(MAX), @cmd_ins_arc NVARCHAR(MAX), @cmd_tmp_upd_arc NVARCHAR(MAX)
declare @cmd_del NVARCHAR(MAX), @cmd_del_tmp NVARCHAR(150), @YeaEnd datetime, @ErrorMSG VARCHAR(1000), @ErrNum INT
declare @ErrSev INT, @ErrState int, @ErrLine int, @ErrProc VARCHAR(200), @DelQueue VARCHAR(200), @vQtime DATETIME

SET @RecordsCnt = 0
SET @DelQueue = 'DELETE FROM [Maintenance].[dbo].[mnttblArchivedTables_RunningQueues] WHERE fQueueID = '+convert(varchar(10),@vQueueID)+';'

if (OBJECT_ID('tempdb..#ArchiveTableList') IS NOT NULL)
	drop table #ArchiveTableList

--CREATE TEMPORARY TABLES IF NOT EXISTS
	BEGIN TRY
		EXEC [dbo].[mnt_sp_ArchiveTables_Create_temptables] @QueueID = @vQueueID
	END TRY
	BEGIN CATCH
		SELECT @ErrorMSG  = ERROR_MESSAGE (), @ErrNum = ERROR_NUMBER(), @ErrSev = ERROR_SEVERITY(), @ErrState = ERROR_STATE(), @ErrLine = ERROR_LINE(), @ErrProc = ERROR_PROCEDURE()
		SET @ErrorMSG  = 'ERROR: Could not create temporary tables: ERROR message: ['+@ErrorMSG+']'
		INSERT INTO [Maintenance].[dbo].[mnttblArchivedTables_RoutineErrors] ([fWritten],[fErrorNumber],[fErrorSeverity],[fErrorState],[fErrorLine],[fErrorProcedure],[fErrorMessage])
			 VALUES (GETDATE(), @ErrNum, @ErrSev, @ErrState, @ErrLine, @ErrProc, @ErrorMSG)
			 EXEC (@DelQueue)
		RAISERROR (@ErrorMSG, 16, 1)
		GOTO end_proc
	END CATCH

CREATE table #ArchiveTableList (fID INT IDENTITY(1,1), fTableName VARCHAR(150), fProcessed bit)

SET @vQtime = GETDATE()
SET @vNow   = CONVERT(DATE,GETDATE())
SET @YeaEnd = CONVERT(varchar(4), YEAR(@vNow))+'-12-31'

insert into #ArchiveTableList (fTableName, fProcessed)
select DISTINCT fTableName, 0 as fProcessed
from [Maintenance].[dbo].[mnttblArchivedTables] with (nolock)
where fDisabled = 0
and fDelete = 0
and fMoveToArchive in (1)
and fMovedToArchive = 0
AND fQueueID = @vQueueID

IF NOT EXISTS ( SELECT TOP(1) fQueueID FROM [Maintenance].[dbo].[mnttblArchivedTables_RunningQueues] WHERE fQueueID = @vQueueID )
	BEGIN
			BEGIN TRY
				BEGIN TRAN
					INSERT INTO [Maintenance].[dbo].[mnttblArchivedTables_RunningQueues]
						([fTableName], [fQueueID],[fTime])
					SELECT fTableName, @vQueueID,  @vQtime  FROM #ArchiveTableList
				COMMIT
			END TRY
			BEGIN CATCH
				SELECT @ErrorMSG  = ERROR_MESSAGE (), @ErrNum = ERROR_NUMBER(), @ErrSev = ERROR_SEVERITY(), @ErrState = ERROR_STATE(), @ErrLine = ERROR_LINE(), @ErrProc = ERROR_PROCEDURE()
				ROLLBACK
				
				SET @ErrorMSG  = 'ERROR: Could not INSERT into table: [Maintenance].[dbo].[mnttblArchivedTables_RunningQueues] Primary table can not be in two different QUEUES. ERROR message: ['+@ErrorMSG+']'
				INSERT INTO [Maintenance].[dbo].[mnttblArchivedTables_RoutineErrors] ([fWritten],[fErrorNumber],[fErrorSeverity],[fErrorState],[fErrorLine],[fErrorProcedure],[fErrorMessage])
					 VALUES (GETDATE(), @ErrNum, @ErrSev, @ErrState, @ErrLine, @ErrProc, @ErrorMSG)
				RAISERROR (@ErrorMSG, 16, 1)
				GOTO end_proc
			END CATCH
	END
ELSE
	BEGIN
				SET @ErrorMSG  = 'ERROR: Another SQL JOB is already runing the same QUEUE ID : ['+convert(varchar(10),@vQueueID)+'] '
				INSERT INTO [Maintenance].[dbo].[mnttblArchivedTables_RoutineErrors] ([fWritten],[fErrorNumber],[fErrorSeverity],[fErrorState],[fErrorLine],[fErrorProcedure],[fErrorMessage])
					 VALUES (GETDATE(), @ErrNum, @ErrSev, @ErrState, @ErrLine, @ErrProc, @ErrorMSG)
				RAISERROR (@ErrorMSG, 16, 1)
				GOTO end_proc
	END


WHILE EXISTS ( SELECT TOP 1 fTableName from #ArchiveTableList where fProcessed = 0)
	begin
			SELECT TOP 1 @vTableName = fTableName from #ArchiveTableList where fProcessed = 0
			
			SELECT top 1 @vfID = fID, @vfDatabaseName= fDatabaseName, @vfSchema = fSchema, @vArchive_recLimit = fArchived_RecordLimit, @vRetentionDays = fRetentionDays, @TotalPKs = fPrimaryKey_Number,
					 @vRetetionDays_ColumnName = fRetentionDays_ColumnName,	@vfArchivedTable = fArchivedTable, @vfStagingTable = fStagingTable, @vDateEnd = fDateEnd, @vDateBegin = fDateBegin,
					 @vPrimaryKey_1 = fPrimaryKey_1, @vPrimaryKey_2 = fPrimaryKey_2, @vPrimaryKey_3 = fPrimaryKey_3, @vPrimaryKey_4 = fPrimaryKey_4, @vArchivedRecordsCnt = fArchivedRecordsCnt,
					 @vSourceTable = @vfDatabaseName+'.'+@vfSchema+'.'+@vTableName
			  FROM [Maintenance].[dbo].[mnttblArchivedTables] with (nolock)
			 WHERE fTableName = @vTableName
			   AND fDateBegin < @vNow --and fDateEnd > @vNow 
			   AND fDelete = 0 and fDisabled = 0
			   AND fMoveToArchive in (1) and fMovedToArchive = 0
			   AND fQueueID = @vQueueID
			   ORDER BY fDateBegin
			   
			IF ( DATEDIFF(DD, @vDateEnd, @vNow) <= @vRetentionDays )
				BEGIN
					SET @vDateEnd = CONVERT(DATE, @vNow - @vRetentionDays) 
				END
			
			PRINT ('   -- Starting table: ['+@vTableName+']: '+convert(varchar(24), getdate(),120))
			 
			set @cmd_del_tmp = 'truncate table '+@vfStagingTable+ CHAR(10)
				
			BEGIN TRY
				 --print @cmd_del_tmp
				exec (@cmd_del_tmp)
			END TRY
			BEGIN CATCH
				SELECT @ErrorMSG  = ERROR_MESSAGE (), @ErrNum = ERROR_NUMBER(), @ErrSev = ERROR_SEVERITY(), @ErrState = ERROR_STATE(), @ErrLine = ERROR_LINE(), @ErrProc = ERROR_PROCEDURE()
				SET @ErrorMSG  = 'ERROR: Could not porcess @cmd_del_tmp command souce table: ['+@vSourceTable+'] dest table ['+@vfStagingTable+'] ERROR message: ['+@ErrorMSG+']'
				INSERT INTO [Maintenance].[dbo].[mnttblArchivedTables_RoutineErrors] ([fWritten],[fErrorNumber],[fErrorSeverity],[fErrorState],[fErrorLine],[fErrorProcedure],[fErrorMessage])
					 VALUES (GETDATE(), @ErrNum, @ErrSev, @ErrState, @ErrLine, @ErrProc, @ErrorMSG)
				EXEC (@DelQueue)
				RAISERROR (@ErrorMSG, 16, 1)
				GOTO end_proc
			END CATCH
			
			set @cmd_tmp_ins = '--POPULATING STG TABLE'+ CHAR(10)
			set @cmd_tmp_ins = @cmd_tmp_ins+'INSERT INTO '+@vfStagingTable + CHAR(10)
			set @cmd_tmp_ins = @cmd_tmp_ins+'SELECT TOP('+convert(varchar(10),@vArchive_recLimit)+') '
			IF ( @TotalPKs = 1 )  set @cmd_tmp_ins = @cmd_tmp_ins+@vPrimaryKey_1+', 0'+ CHAR(10)
			IF ( @TotalPKs = 2 )  set @cmd_tmp_ins = @cmd_tmp_ins+@vPrimaryKey_1+', '+@vPrimaryKey_2+', 0'+ CHAR(10)
			IF ( @TotalPKs = 3 )  set @cmd_tmp_ins = @cmd_tmp_ins+@vPrimaryKey_1+', '+@vPrimaryKey_2+', '+@vPrimaryKey_3+', 0'+ CHAR(10)
			IF ( @TotalPKs = 4 )  set @cmd_tmp_ins = @cmd_tmp_ins+@vPrimaryKey_1+', '+@vPrimaryKey_2+', '+@vPrimaryKey_3+', '+@vPrimaryKey_4+', 0'+ CHAR(10)
			set @cmd_tmp_ins = @cmd_tmp_ins+'  FROM '+@vfDatabaseName+'.'+@vfSchema+'.'+@vTableName+' with (nolock)'+ CHAR(10)
			set @cmd_tmp_ins = @cmd_tmp_ins+' WHERE '+@vRetetionDays_ColumnName+' < '''+CONVERT(VARCHAR(24), @vDateEnd,120)+''''+ CHAR(10)
			set @cmd_tmp_ins = @cmd_tmp_ins+'   AND '+@vRetetionDays_ColumnName+' >= '''+CONVERT(VARCHAR(24), @vDateBegin,120)+''''+ CHAR(10)
			
			--print @cmd_tmp_ins
			BEGIN TRY
				exec (@cmd_tmp_ins)
				SET @RecordsCnt = @@ROWCOUNT
			END TRY
			BEGIN CATCH
				SELECT @ErrorMSG  = ERROR_MESSAGE (), @ErrNum = ERROR_NUMBER(), @ErrSev = ERROR_SEVERITY(), @ErrState = ERROR_STATE(), @ErrLine = ERROR_LINE(), @ErrProc = ERROR_PROCEDURE()
				SET @ErrorMSG  = 'ERROR: Could not porcess @cmd_tmp_ins command souce table: ['+@vSourceTable+'] dest table ['+@vfStagingTable+'] ERROR message: ['+@ErrorMSG+']'
				INSERT INTO [Maintenance].[dbo].[mnttblArchivedTables_RoutineErrors] ([fWritten],[fErrorNumber],[fErrorSeverity],[fErrorState],[fErrorLine],[fErrorProcedure],[fErrorMessage])
					 VALUES (GETDATE(), @ErrNum, @ErrSev, @ErrState, @ErrLine, @ErrProc, @ErrorMSG)
					 EXEC (@DelQueue)
				RAISERROR (@ErrorMSG, 16, 1)
				GOTO end_proc
			END CATCH
			
			--PRINT '-- Record Count ['+ convert(varchar(10),@RecordsCnt)+']'+ CHAR(10)
			IF (@RecordsCnt = 0 AND (DATEDIFF(DD, @vDateEnd, @vNow) > @vRetentionDays))
				BEGIN
					update [Maintenance].[dbo].[mnttblArchivedTables]
					   set fMoveToArchive = 0, fMovedToArchive = 1
					 where fID = @vfID
					
					PRINT '		# Archive Process completed for table ['+@vTableName+'] begin date:'+''+ CHAR(10)
					goto no_data_toProcess
				END
			
			
			IF (@RecordsCnt = 0)
				BEGIN 
					PRINT '		# No Data to process for table ['+@vTableName+']'+ CHAR(10)
					GOTO no_data_toProcess
				END 
			ELSE
				BEGIN
					SET @vArchivedRecordsCnt = isnull(@vArchivedRecordsCnt,0) + @RecordsCnt
				END
			
						
			set @cmd_tmp_upd_dups = '--CHECK FOR DUPLICATE RECORDS '+ CHAR(10)
			set @cmd_tmp_upd_dups = @cmd_tmp_upd_dups+'UPDATE stg '+ CHAR(10)
			set @cmd_tmp_upd_dups = @cmd_tmp_upd_dups+'   SET stg.fProcessed = 1'+ CHAR(10)
			set @cmd_tmp_upd_dups = @cmd_tmp_upd_dups+'  FROM '+@vfStagingTable+' stg '+ CHAR(10)
			set @cmd_tmp_upd_dups = @cmd_tmp_upd_dups+' INNER JOIN '+@vfArchivedTable+' arc with (nolock)'+ CHAR(10)
			IF ( @TotalPKs = 1 )  set @cmd_tmp_upd_dups = @cmd_tmp_upd_dups+'    ON stg.'+@vPrimaryKey_1+' = arc.'+@vPrimaryKey_1+ CHAR(10)
			IF ( @TotalPKs = 2 )  set @cmd_tmp_upd_dups = @cmd_tmp_upd_dups+'    ON stg.'+@vPrimaryKey_1+' = arc.'+@vPrimaryKey_1+' AND stg.'+@vPrimaryKey_2+' = arc.'+@vPrimaryKey_2+ CHAR(10)
			IF ( @TotalPKs = 3 )  set @cmd_tmp_upd_dups = @cmd_tmp_upd_dups+'    ON stg.'+@vPrimaryKey_1+' = arc.'+@vPrimaryKey_1+' AND stg.'+@vPrimaryKey_2+' = arc.'+@vPrimaryKey_2+' AND stg.'+@vPrimaryKey_3+' = arc.'+@vPrimaryKey_3+ CHAR(10)
			IF ( @TotalPKs = 4 )  set @cmd_tmp_upd_dups = @cmd_tmp_upd_dups+'    ON stg.'+@vPrimaryKey_1+' = arc.'+@vPrimaryKey_1+' AND stg.'+@vPrimaryKey_2+' = arc.'+@vPrimaryKey_2+' AND stg.'+@vPrimaryKey_3+' = arc.'+@vPrimaryKey_3+' AND stg.'+@vPrimaryKey_4+' = arc.'+@vPrimaryKey_4+ CHAR(10)
			
			--print @cmd_tmp_upd_dups
			BEGIN TRY
				exec(@cmd_tmp_upd_dups)
			END TRY
			BEGIN CATCH
				SELECT @ErrorMSG  = ERROR_MESSAGE (), @ErrNum = ERROR_NUMBER(), @ErrSev = ERROR_SEVERITY(), @ErrState = ERROR_STATE(), @ErrLine = ERROR_LINE(), @ErrProc = ERROR_PROCEDURE()
				SET @ErrorMSG  = 'ERROR: Could not porcess @cmd_tmp_upd_dups command on table: ['+@vfStagingTable+'] ERROR message: ['+@ErrorMSG+']'
				INSERT INTO [Maintenance].[dbo].[mnttblArchivedTables_RoutineErrors] ([fWritten],[fErrorNumber],[fErrorSeverity],[fErrorState],[fErrorLine],[fErrorProcedure],[fErrorMessage])
					 VALUES (GETDATE(), @ErrNum, @ErrSev, @ErrState, @ErrLine, @ErrProc, @ErrorMSG)
					 EXEC (@DelQueue)
				RAISERROR (@ErrorMSG, @ErrSev, @ErrState)
				GOTO end_proc
			END CATCH
			

			SET @cmd_ins_arc = '-- ARCHIVING RECORDS'+ CHAR(10)
			SET @cmd_ins_arc = @cmd_ins_arc+'INSERT INTO '+ @vfArchivedTable + CHAR(10)
			SET @cmd_ins_arc = @cmd_ins_arc+'     SELECT cur.*'+ CHAR(10)
			SET @cmd_ins_arc = @cmd_ins_arc+'       FROM '+@vfDatabaseName+'.'+@vfSchema+'.'+@vTableName+' cur with (nolock)'+ CHAR(10)
			SET @cmd_ins_arc = @cmd_ins_arc+'      INNER JOIN '+@vfStagingTable+' stg '+ CHAR(10)
			IF ( @TotalPKs = 1 )  SET @cmd_ins_arc = @cmd_ins_arc+'         ON stg.'+@vPrimaryKey_1+' = cur.'+@vPrimaryKey_1+ CHAR(10)
			IF ( @TotalPKs = 2 )  SET @cmd_ins_arc = @cmd_ins_arc+'         ON stg.'+@vPrimaryKey_1+' = cur.'+@vPrimaryKey_1+' AND stg.'+@vPrimaryKey_2+' = cur.'+@vPrimaryKey_2+ CHAR(10)
			IF ( @TotalPKs = 3 )  SET @cmd_ins_arc = @cmd_ins_arc+'         ON stg.'+@vPrimaryKey_1+' = cur.'+@vPrimaryKey_1+' AND stg.'+@vPrimaryKey_2+' = cur.'+@vPrimaryKey_2+' AND stg.'+@vPrimaryKey_3+' = cur.'+@vPrimaryKey_3+ CHAR(10)
			IF ( @TotalPKs = 4 )  SET @cmd_ins_arc = @cmd_ins_arc+'         ON stg.'+@vPrimaryKey_1+' = cur.'+@vPrimaryKey_1+' AND stg.'+@vPrimaryKey_2+' = cur.'+@vPrimaryKey_2+' AND stg.'+@vPrimaryKey_3+' = cur.'+@vPrimaryKey_3+' AND stg.'+@vPrimaryKey_4+' = cur.'+@vPrimaryKey_4+ CHAR(10)
			SET @cmd_ins_arc = @cmd_ins_arc+'      WHERE stg.fProcessed = 0  --> only new records'+ CHAR(10)

			--print @cmd_ins_arc
			BEGIN TRY
				exec(@cmd_ins_arc)
			END TRY
			BEGIN CATCH
				SELECT @ErrorMSG  = ERROR_MESSAGE (), @ErrNum = ERROR_NUMBER(), @ErrSev = ERROR_SEVERITY(), @ErrState = ERROR_STATE(), @ErrLine = ERROR_LINE(), @ErrProc = ERROR_PROCEDURE()
				SET @ErrorMSG  = 'ERROR: Could not porcess @cmd_ins_arc command source table: ['+@vSourceTable+'] dest table ['+@vfArchivedTable+'] ERROR message: ['+@ErrorMSG+']'
				INSERT INTO [Maintenance].[dbo].[mnttblArchivedTables_RoutineErrors] ([fWritten],[fErrorNumber],[fErrorSeverity],[fErrorState],[fErrorLine],[fErrorProcedure],[fErrorMessage])
					 VALUES (GETDATE(), @ErrNum, @ErrSev, @ErrState, @ErrLine, @ErrProc, @ErrorMSG)
					 EXEC (@DelQueue)
				RAISERROR (@ErrorMSG, @ErrSev, @ErrState)
				GOTO end_proc
			END CATCH

			SET @cmd_tmp_upd_arc = '-- TEMP TABLE UPDATE ARCHIVED RECORDS'+ CHAR(10)
			SET @cmd_tmp_upd_arc = @cmd_tmp_upd_arc+'UPDATE stg '+ CHAR(10)
			SET @cmd_tmp_upd_arc = @cmd_tmp_upd_arc+'   SET stg.fProcessed = 1'+ CHAR(10)
			SET @cmd_tmp_upd_arc = @cmd_tmp_upd_arc+'  FROM '+@vfStagingTable+' stg '+ CHAR(10)
			SET @cmd_tmp_upd_arc = @cmd_tmp_upd_arc+' INNER JOIN '+@vfArchivedTable+' arc with (nolock)'+ CHAR(10)
			IF ( @TotalPKs = 1 )  SET @cmd_tmp_upd_arc = @cmd_tmp_upd_arc+'    ON stg.'+@vPrimaryKey_1+' = arc.'+@vPrimaryKey_1+ CHAR(10)
			IF ( @TotalPKs = 2 )  SET @cmd_tmp_upd_arc = @cmd_tmp_upd_arc+'    ON stg.'+@vPrimaryKey_1+' = arc.'+@vPrimaryKey_1+' AND stg.'+@vPrimaryKey_2+' = arc.'+@vPrimaryKey_2+ CHAR(10)
			IF ( @TotalPKs = 3 )  SET @cmd_tmp_upd_arc = @cmd_tmp_upd_arc+'    ON stg.'+@vPrimaryKey_1+' = arc.'+@vPrimaryKey_1+' AND stg.'+@vPrimaryKey_2+' = arc.'+@vPrimaryKey_2+' AND stg.'+@vPrimaryKey_3+' = arc.'+@vPrimaryKey_3+ CHAR(10)
			IF ( @TotalPKs = 4 )  SET @cmd_tmp_upd_arc = @cmd_tmp_upd_arc+'    ON stg.'+@vPrimaryKey_1+' = arc.'+@vPrimaryKey_1+' AND stg.'+@vPrimaryKey_2+' = arc.'+@vPrimaryKey_2+' AND stg.'+@vPrimaryKey_3+' = arc.'+@vPrimaryKey_3+' AND stg.'+@vPrimaryKey_4+' = arc.'+@vPrimaryKey_4+ CHAR(10)
			SET @cmd_tmp_upd_arc = @cmd_tmp_upd_arc+' WHERE stg.fProcessed = 0'+ CHAR(10)
			
			--print @cmd_tmp_upd_arc
			BEGIN TRY
				exec(@cmd_tmp_upd_arc)
			END TRY
			BEGIN CATCH
				SELECT @ErrorMSG  = ERROR_MESSAGE (), @ErrNum = ERROR_NUMBER(), @ErrSev = ERROR_SEVERITY(), @ErrState = ERROR_STATE(), @ErrLine = ERROR_LINE(), @ErrProc = ERROR_PROCEDURE()
				SET @ErrorMSG  = 'ERROR: Could not porcess @@cmd_tmp_upd_arc command on table: ['+@vfStagingTable+'] ERROR message: ['+@ErrorMSG+']'
				INSERT INTO [Maintenance].[dbo].[mnttblArchivedTables_RoutineErrors] ([fWritten],[fErrorNumber],[fErrorSeverity],[fErrorState],[fErrorLine],[fErrorProcedure],[fErrorMessage])
					 VALUES (GETDATE(), @ErrNum, @ErrSev, @ErrState, @ErrLine, @ErrProc, @ErrorMSG)
					 EXEC (@DelQueue)
				RAISERROR (@ErrorMSG, @ErrSev, @ErrState)
				GOTO end_proc
			END CATCH
			
			set @cmd_del = '-- DELETE ARCHIVED RECORDS FROM MAIN TABLE'+ CHAR(10)
			SET @cmd_del = @cmd_del+'DELETE cur '+ CHAR(10)
			SET @cmd_del = @cmd_del+'  FROM '+@vfDatabaseName+'.'+@vfSchema+'.'+@vTableName+' cur '+ CHAR(10)
			SET @cmd_del = @cmd_del+'  INNER JOIN '+@vfStagingTable+' stg '+ CHAR(10)
			IF ( @TotalPKs = 1 )  SET @cmd_del = @cmd_del+'         ON stg.'+@vPrimaryKey_1+' = cur.'+@vPrimaryKey_1+ CHAR(10)
			IF ( @TotalPKs = 2 )  SET @cmd_del = @cmd_del+'         ON stg.'+@vPrimaryKey_1+' = cur.'+@vPrimaryKey_1+' AND stg.'+@vPrimaryKey_2+' = cur.'+@vPrimaryKey_2+ CHAR(10)
			IF ( @TotalPKs = 3 )  SET @cmd_del = @cmd_del+'         ON stg.'+@vPrimaryKey_1+' = cur.'+@vPrimaryKey_1+' AND stg.'+@vPrimaryKey_2+' = cur.'+@vPrimaryKey_2+' AND stg.'+@vPrimaryKey_3+' = cur.'+@vPrimaryKey_3+ CHAR(10)
			IF ( @TotalPKs = 4 )  SET @cmd_del = @cmd_del+'         ON stg.'+@vPrimaryKey_1+' = cur.'+@vPrimaryKey_1+' AND stg.'+@vPrimaryKey_2+' = cur.'+@vPrimaryKey_2+' AND stg.'+@vPrimaryKey_3+' = cur.'+@vPrimaryKey_3+' AND stg.'+@vPrimaryKey_4+' = cur.'+@vPrimaryKey_4+ CHAR(10)
			SET @cmd_del = @cmd_del+'  WHERE stg.fProcessed = 1'+ CHAR(10)
			
			--print @cmd_del
			BEGIN TRY
				exec(@cmd_del)
			END TRY
			BEGIN CATCH
				SELECT @ErrorMSG  = ERROR_MESSAGE (), @ErrNum = ERROR_NUMBER(), @ErrSev = ERROR_SEVERITY(), @ErrState = ERROR_STATE(), @ErrLine = ERROR_LINE(), @ErrProc = ERROR_PROCEDURE()
				SET @ErrorMSG  = 'ERROR: Could not porcess @cmd_del command on table: ['+@vSourceTable+'] ERROR message: ['+@ErrorMSG+']'
				INSERT INTO [Maintenance].[dbo].[mnttblArchivedTables_RoutineErrors] ([fWritten],[fErrorNumber],[fErrorSeverity],[fErrorState],[fErrorLine],[fErrorProcedure],[fErrorMessage])
					 VALUES (GETDATE(), @ErrNum, @ErrSev, @ErrState, @ErrLine, @ErrProc, @ErrorMSG)
				EXEC (@DelQueue)
				RAISERROR (@ErrorMSG, @ErrSev, @ErrState)
				GOTO end_proc
			END CATCH
			
			update [Maintenance].[dbo].[mnttblArchivedTables]
			   set fArchivedRecordsCnt = @vArchivedRecordsCnt, fTime = GETDATE()
			 where fID = @vfID
			
			no_data_toProcess:
			
			print @cmd_del_tmp
			exec (@cmd_del_tmp)
			update #ArchiveTableList set fProcessed = 1 where fTableName = @vTableName
			SET @RecordsCnt = 0
			PRINT ('   -- End table: ['+@vTableName+']: '+convert(varchar(24), getdate(),120))
			
	end

--Cleaning runing queue
EXEC (@DelQueue)

end_proc:
END

GO
/****** Object:  StoredProcedure [dbo].[mnt_sp_ArchiveTables_Activate_NewPeriods]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
Versin History:
	2015-12-20	Mario.O		Created Procedure to Activate new archive periods
	2016-10-14  Mario.O		Update proce to check all archive persiods not just the current year

*/
CREATE PROCEDURE [dbo].[mnt_sp_ArchiveTables_Activate_NewPeriods]
AS
BEGIN
SET NOCOUNT ON;

IF NOT EXISTS(SELECT TOP(1) fID FROM [Maintenance].[dbo].[mnttblArchivedTables] with (nolock)
				WHERE fDisabled = 0
				  and fDelete = 0 )
	BEGIN
		PRINT ' -- Nothing to Process'
		GOTO end_proc
	END
	
		IF ( OBJECT_ID('tempdb..#TableList') IS NOT NULL )
			BEGIN DROP TABLE #TableList; END;

		CREATE TABLE #TableList (fID INT, fTableName VARCHAR(500), fDateBegin DATETIME, fDateEnd DATETIME, fRetentionDay DATETIME, fPorcessed INT)
		DECLARE @vCurentDate datetime, @vCurrentYear INT, @vfID INT, @vTableName VARCHAR(500), @vBeginDate DATETIME, @vServerName VARCHAR(100)
		DECLARE @vRecCnt INT

		SELECT @vCurrentYear = YEAR(GETDATE()), @vCurentDate  = CONVERT(DATE,GETDATE()), @vServerName = @@SERVERNAME

		INSERT INTO #TableList
		SELECT [fID],[fTableName], [fDateBegin], [fDateEnd], 
				CONVERT(DATETIME, @vCurentDate - [fRetentionDays]), 0
		  FROM [Maintenance].[dbo].[mnttblArchivedTables] WITH (NOLOCK)
		WHERE fMoveToArchive = 0 AND fMovedToArchive = 0
		  AND [fDisabled]    = 0 AND [fDelete] = 0
		  AND [fTableName] NOT IN ( SELECT DISTINCT [fTableName] -- ONLY GET TABLES THAT HAVE NO ACTIVE ARCHIVE PERIODS
									  FROM [Maintenance].[dbo].[mnttblArchivedTables] with (nolock)
									 WHERE fMoveToArchive = 1 AND fMovedToArchive = 0 AND [fDisabled] = 0 
									   AND [fDelete] = 0
									   )
		  ORDER BY [fDateBegin] ASC
		
		SET @vRecCnt = @@ROWCOUNT

		IF @vRecCnt = 0
			GOTO end_proc

		-- REMOVE ANY PERIODS THAT ARE IN THE FUTURE
		DELETE FROM #TableList WHERE fRetentionDay < [fDateBegin]

		  WHILE EXISTS (SELECT TOP(1) [fTableName] FROM #TableList WHERE fPorcessed = 0  )
			BEGIN
				-- process records
				SELECT TOP(1) @vfID = [fID], @vTableName = [fTableName]
				  FROM #TableList WHERE fPorcessed = 0 
				 ORDER BY [fDateBegin] ASC
				
				-- Avtivate new period
				UPDATE [Maintenance].[dbo].[mnttblArchivedTables]
				   SET fMoveToArchive = 1
				WHERE [fID] = @vfID
				  AND fMoveToArchive = 0
				  AND fMovedToArchive = 0
				  AND [fDisabled] = 0
				  AND [fDelete] = 0
		
				-- ACTIVATE only one period at the time for each primary table
				UPDATE #TableList SET fPorcessed =  1 where [fID] = @vfID
				UPDATE #TableList SET fPorcessed = -2 where [fTableName] = @vTableName AND [fID] <> @vfID
		END

		 IF EXISTS (select top 1 [fTableName] from #TableList WHERE fPorcessed = 1)
			 BEGIN
		 
				 DECLARE @vcmd varchar(2000), @vMaxLen INT, @vSpaces VARCHAR(20), @subject VARCHAR(200)
				 SELECT @vMaxLen = MAX(LEN([fTableName])) FROM #TableList

				 SET @subject = '['+@vServerName+'] - New Archive Periods were Activated'
				 SET @vcmd = '<P>'+@vServerName+' - PERIODS ACTIVATED FOR TABLES</P>'+char(10)+char(10)
		 
				 SELECT @vcmd = @vcmd+'<pre>  - ['+left([fTableName]+']'+REPLICATE(' ', 100), @vMaxLen+5)+'  Archive Period Activated:	'+convert(VARCHAR(10),[fDateBegin],120)+' '+CHAR(10)
				   FROM #TableList WHERE fPorcessed = 1
				   SET @vcmd = @vcmd+'</pre>'
				   
				     exec  msdb..sp_send_dbmail  
							  @profile_name =  'SQLAlerts'
							, @recipients  = 'Ria_DBSuport@riafinancial.com' 
							, @body = @vcmd
							, @subject = @subject
							, @body_format = 'HTML'
							, @importance ='NORMAL' 

			 END

end_proc:
		--cleanup
		IF ( OBJECT_ID('tempdb..#TableList') IS NOT NULL )
			BEGIN DROP TABLE #TableList; END;
END



GO
/****** Object:  StoredProcedure [dbo].[mnt_sp_ArchiveTables_Create_temptables]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[mnt_sp_ArchiveTables_Create_temptables]
		( @QueueID INT = 0 )
AS
BEGIN
	DECLARE @vDBName VARCHAR(128),@vfSchema VARCHAR(128),@vfTableName VARCHAR(128),@vStageTable VARCHAR(500)
	DECLARE @vPKcnt INT, @vPK1 VARCHAR(100), @vPK2 VARCHAR(100), @vPK3 VARCHAR(100), @vPK4 VARCHAR(100)
	DECLARE @slqcmd VARCHAR(MAX)

	SET NOCOUNT ON;

	IF (OBJECT_ID('tempdb..#TblTemp') IS NOT NULL)
		begin DROP TABLE #TblTemp end

	CREATE TABLE #TblTemp ([fDatabaseName] VARCHAR(128),[fSchema] VARCHAR(128),[fTableName] VARCHAR(128),[fStagingTable] VARCHAR(500), PKcnt INT, PK1 VARCHAR(100), PK2 VARCHAR(100), PK3 VARCHAR(100), PK4 VARCHAR(100), fProcessed bit)

	INSERT INTO #TblTemp
	SELECT DISTINCT [fDatabaseName],[fSchema],[fTableName],[fStagingTable],[fPrimaryKey_Number],[fPrimaryKey_1],
		[fPrimaryKey_2],[fPrimaryKey_3],[fPrimaryKey_4],0 AS fProcessed
	  FROM [Maintenance].[dbo].[mnttblArchivedTables]
	  WHERE fQueueID = CASE WHEN @QueueID = 0 THEN fQueueID ELSE @QueueID END
	  
	WHILE EXISTS(SELECT TOP(1) PKcnt FROM #TblTemp WHERE fProcessed = 0 )
		BEGIN
			SELECT TOP(1) @vDBName = [fDatabaseName], @vfSchema = [fSchema], @vfTableName = [fTableName], 
					@vStageTable = [fStagingTable], @vPKcnt = PKcnt, 
					@vPK1 = PK1, @vPK2 = PK2, @vPK3 = PK3, @vPK4 = PK4
			  FROM #TblTemp WHERE fProcessed = 0
			  
			  SET @slqcmd = '-- CHECK TABLE ['+@vStageTable+']'+CHAR(10)
						+'IF (OBJECT_ID('''+@vStageTable+''') IS NULL)'+CHAR(10)
						+CHAR(9)+'BEGIN'+CHAR(10)
						+CHAR(9)+CHAR(9)+'SELECT TOP(1) '
						IF ISNULL(@vPKcnt,0) = 1 SET @slqcmd = @slqcmd + @vPK1 + ', CAST(0 as BIT) as fProcessed '+CHAR(10)
						IF ISNULL(@vPKcnt,0) = 2 SET @slqcmd = @slqcmd + @vPK1 +', '+ @vPK2 + ', CAST(0 as BIT) as fProcessed '+CHAR(10)
						IF ISNULL(@vPKcnt,0) = 3 SET @slqcmd = @slqcmd + @vPK1 +', '+ @vPK2 +', '+ @vPK3 +  ', CAST(0 as BIT) as fProcessed '+CHAR(10)
						IF ISNULL(@vPKcnt,0) = 4 SET @slqcmd = @slqcmd + @vPK1 +', '+ @vPK2 +', '+ @vPK3 +', '+ @vPK4 +  ', CAST(0 as BIT) as fProcessed '+CHAR(10)
			  SET @slqcmd = @slqcmd +CHAR(9)+CHAR(9)+'  INTO '+@vStageTable+CHAR(10)
						+CHAR(9)+CHAR(9)+'  FROM ['+@vDBName+'].['+@vfSchema+'].['+@vfTableName+']'+CHAR(10)
						+CHAR(9)+CHAR(9)+' WHERE 1 = 2'+CHAR(10)+CHAR(10)
						+CHAR(9)+CHAR(9)+'ALTER TABLE '+@vStageTable+' ADD CONSTRAINT PK_stg_'+@vfTableName+' PRIMARY KEY ('
						IF ISNULL(@vPKcnt,0) = 1 SET @slqcmd = @slqcmd + @vPK1 + ')'+CHAR(10)
						IF ISNULL(@vPKcnt,0) = 2 SET @slqcmd = @slqcmd + @vPK1 +', '+ @vPK2 + ')'+CHAR(10)
						IF ISNULL(@vPKcnt,0) = 3 SET @slqcmd = @slqcmd + @vPK1 +', '+ @vPK2 +', '+ @vPK3 +  ')'+CHAR(10)
						IF ISNULL(@vPKcnt,0) = 4 SET @slqcmd = @slqcmd + @vPK1 +', '+ @vPK2 +', '+ @vPK3 +', '+ @vPK4 +  ')'+CHAR(10)
			  SET @slqcmd = @slqcmd + CHAR(9)+CHAR(9)+'PRINT ''  -- Temp Table "'+@vStageTable+'" created'' '+CHAR(10)
						+CHAR(9)+'END;'+CHAR(10)
			
			--PRINT(@slqcmd)
			EXEC(@slqcmd)

			UPDATE #TblTemp SET fProcessed = 1 WHERE [fStagingTable] = @vStageTable
		END
END
GO
/****** Object:  StoredProcedure [dbo].[mnt_sp_GetCounterDetail]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[mnt_sp_GetCounterDetail]
    @CounterDate datetime = NULL,
	@CounterName varchar(50),
	@ServerName varchar(255)
as

--declare @CounterTimeStart datetime, @CounterTimeEnd datetime
--set @CounterDate = NULL

if @CounterDate IS NULL
begin
  set @CounterDate = CONVERT(VARCHAR(10),GETDATE(),101)
end

select *
from [mnttblCounterDetails_All] 
where CounterDateTime >= @CounterDate 
  and CounterDateTime < @CounterDate + 1
  and counterName = @CounterName
  and ServerName = @ServerName 
ORDER BY CounterDAteTime
GO
/****** Object:  StoredProcedure [dbo].[mnt_sp_GetCounterDetails_All]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[mnt_sp_GetCounterDetails_All]
 @CounterDate datetime = NULL,
 @ServerName varchar(100) = NULL  
 
as

--% Processor Time
--Current Disk Queue Length
--Page life expectancy
--Pages/sec
--User Connections

--   Exec [mnt_sp_GetCounterDetails_All] @CounterDate = '9/2/2015' '% Processor Time', @ServerName = ''

--declare  @CounterDate datetime = '6/4/2015' --NULL
--DECLARE @CounterName varchar(100) = '% Processor Time'

--delete from Maintenance..tmpOut
 
declare @CounterTimeStart datetime, @CounterTimeEnd datetime 

if @CounterDate IS NULL
begin
  set @CounterDate = CONVERT(VARCHAR(10),GETDATE(),101)
end

DECLARE @Last14 bit 
set @Last14 = 0

If @Last14 = 1
begin
	set @CounterTimeStart = dateadd(day, -14, @CounterDAte)
	set @CounterTimeEnd = dateadd(hour, 24, @CounterDate)
	set @CounterTimeEnd = dateadd(minute, -1, @CounterTimeEnd)
end
else
begin
	set @CounterTimeStart = @CounterDAte --dateadd(hour, 1, @CounterDAte)
	set @CounterTimeEnd = dateadd(hour, 24, @CounterDate)
	set @CounterTimeEnd = dateadd(minute, -1, @CounterTimeEnd)
end

--select @CounterDAte, @CounterDate + 1, @CounterTimeStart, @CounterTimeEnd

declare @ExecTime datetime
set @exectime = getdate()

--create table Maintenance..tmp (ExecTime datetime, CounterDate datetime, Average float, CounterName varchar(55), ServerName varchar(255))
create table #tmp (ExecTime datetime, CounterDate datetime, CounterValue float, CounterName varchar(55), ServerName varchar(255))


--insert into Maintenance..tmp 
If @ServerName IS NULL
BEGIN
	insert into #tmp
	select @exectime, CounterDateTime, CounterValue, Case When CounterName = '% Processor Time' Then 'Processor Time' else CounterName End As CounterName, ServerName
	from [mnttblCounterDetails_All] with (nolock)
	where CounterDateTime between @CounterTimeStart and @CounterTimeEnd 
	  --and datepart(minute, CounterDateTime) in (0, 5, 10)--, 15, 20, 25, 30, 35, 40, 45, 50, 55)
	  --and CounterName = @CounterName
	order by ServerName, CounterDateTime
END
ELSE
BEGIN
	insert into #tmp
	select @exectime, CounterDateTime, CounterValue, Case When CounterName = '% Processor Time' Then 'Processor Time' else CounterName End As CounterName, ServerName
	from [mnttblCounterDetails_All] with (nolock)
	where CounterDateTime between @CounterTimeStart and @CounterTimeEnd 
	  And ServerName = @ServerName 
	  --and datepart(minute, CounterDateTime) in (0, 5, 10)--, 15, 20, 25, 30, 35, 40, 45, 50, 55)
	  --and CounterName = @CounterName
	order by ServerName, CounterDateTime

END 

select * from #tmp
order by ServerName, CounterDate
GO
/****** Object:  StoredProcedure [dbo].[mnt_sp_GetCounterDetails_All_ByCounter]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE proc [dbo].[mnt_sp_GetCounterDetails_All_ByCounter]
   @CounterDate datetime = NULL,
   @ServerName varchar(100) = NULL,
   @CounterName varchar(100) 
 
AS

--% Processor Time
--Current Disk Queue Length
--Page life expectancy
--Pages/sec
--User Connections

-- Exec mnt_sp_GetCounterDetails_All_ByCounter @CounterDate = '9/12/2016', @ServerName = 'ATLAS',  @CounterName = '%User Connections%'
-- Exec mnt_sp_GetCounterDetails_All_ByCounter @CounterDate = '9/12/2016', @CounterName = '%Processor%'

--declare  @CounterDate datetime = '6/4/2015' --NULL
--DECLARE @CounterName varchar(100) = '% Processor Time'

--delete from Maintenance..tmpOut
 
declare @CounterTimeStart datetime, @CounterTimeEnd datetime 

if @CounterDate IS NULL
begin
  set @CounterDate = CONVERT(VARCHAR(10),GETDATE(),101)
  --set @CounterDate = '5/24/2019'
end

DECLARE @Last14 bit 
set @Last14 = 0

If @Last14 = 1
begin
	set @CounterTimeStart = dateadd(day, -14, @CounterDAte)
	set @CounterTimeEnd = dateadd(hour, 24, @CounterDate)
	set @CounterTimeEnd = dateadd(minute, -1, @CounterTimeEnd)
end
else
begin
	set @CounterTimeStart = @CounterDAte --dateadd(hour, 1, @CounterDAte)
	set @CounterTimeEnd = dateadd(hour, 24, @CounterDate)
	set @CounterTimeEnd = dateadd(minute, -1, @CounterTimeEnd)
end

--select @CounterDAte, @CounterDate + 1, @CounterTimeStart, @CounterTimeEnd

declare @ExecTime datetime
set @exectime = getdate()

--create table Maintenance..tmp (ExecTime datetime, CounterDate datetime, Average float, CounterName varchar(55), ServerName varchar(255))
create table #tmp ( CounterDate datetime, CounterValue float, CounterName varchar(255) )


--insert into Maintenance..tmp 
If @ServerName IS NULL
BEGIN
	insert into #tmp
	Select CounterDateTime, CounterValue, ServerName + ':' + RTRIM(CounterName)  
	From [mnttblCounterDetails_All] with (nolock)
	Where CounterDateTime between @CounterTimeStart and @CounterTimeEnd
	  and CounterName like @CounterName
  
END
ELSE
BEGIN
	insert into #tmp
	Select CounterDateTime, CounterValue, ServerName + ':' + RTRIM(CounterName)
	From [mnttblCounterDetails_All] with (nolock)
	Where CounterDateTime between @CounterTimeStart and @CounterTimeEnd
	  and CounterName like @CounterName
	  And ServerName = @ServerName  

END 

select * from #tmp
order by CounterName, CounterDate
 

GO
/****** Object:  StoredProcedure [dbo].[mnt_sp_GetCounterSummary]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[mnt_sp_GetCounterSummary]
  @CounterDate datetime = NULL
as

delete from Maintenance..tmpOut

--declare @CounterDate datetime
declare @CounterTimeStart datetime, @CounterTimeEnd datetime
--set @CounterDate = NULL

if @CounterDate IS NULL or @CounterDate = '1/1/1900'
begin
  set @CounterDate = CONVERT(VARCHAR(10),GETDATE(),101)
end

set @CounterTimeStart = dateadd(hour, 1, @CounterDAte)
set @CounterTimeEnd = dateadd(hour, 23, @CounterDate)

--select @CounterDAte, @CounterDate + 1, @CounterTimeStart, @CounterTimeEnd
declare @ExecTime datetime
set @exectime = getdate()

--create table Maintenance..tmp (ExecTime datetime, CounterDate datetime, Average float, CounterName varchar(55), ServerName varchar(255))

insert into Maintenance..tmp 
select @exectime, @CounterDate As CounterDate, sum(CounterValue) / Count(*) As Average, CounterName, ServerName
from [mnttblCounterDetails_All] 
where CounterDateTime between @CounterTimeStart and @CounterTimeEnd
group by CounterName, ServerName

--create table Maintenance..tmpOut (ExecTime datetime, ServerName varchar(255), CounterDAte datetime, AvgCPU float, AvgBCHR float, AvgPLE float, AvgPage float, AvgTran float, AvgUserConn float)

insert into Maintenance..tmpOut (ExecTime, ServerName, counterDate)
select distinct Exectime, ServerName, CounterDAte 
from Maintenance..tmp
where Exectime = @exectime
 
update o set AvgCPU = t.Average
from Maintenance..tmpOut o 
  join Maintenance..tmp t on t.ServerName = o.ServerName and t.ExecTime = o.ExecTime
where CounterName = '% Processor Time'
  and t.Exectime = @exectime

update o set AvgBCHR = t.Average
from Maintenance..tmpOut o 
  join Maintenance..tmp t on t.ServerName = o.ServerName and t.ExecTime = o.ExecTime
where CounterName = 'Buffer cache hit ratio'
  and t.Exectime = @exectime

update o set AvgPLE = t.Average
from Maintenance..tmpOut o 
  join Maintenance..tmp t on t.ServerName = o.ServerName and t.ExecTime = o.ExecTime
where CounterName = 'Page life expectancy'
  and t.Exectime = @exectime

update o set AvgPage = t.Average
from Maintenance..tmpOut o 
  join Maintenance..tmp t on t.ServerName = o.ServerName and t.ExecTime = o.ExecTime
where CounterName = 'Pages/sec'
  and t.Exectime = @exectime

update o set AvgTran = t.Average
from Maintenance..tmpOut o 
  join Maintenance..tmp t on t.ServerName = o.ServerName and t.ExecTime = o.ExecTime
where CounterName = 'Transactions/sec' 
   and t.Exectime = @exectime

 update o set AvgUserConn = t.Average
from Maintenance..tmpOut o 
  join Maintenance..tmp t on t.ServerName = o.ServerName and t.ExecTime = o.ExecTime
where CounterName = 'User Connections' 
  and t.Exectime = @exectime
 
select Exectime, ServerName, CONVERT(VARCHAR(10),CounterDate,101) as CounterDate, AvgCPU, AvgBCHR, AvgPLE, AvgPage
	, AvgTran, AvgUserConn
from Maintenance..tmpOut
where Exectime = @ExecTime
GO
/****** Object:  StoredProcedure [dbo].[mnt_sp_GetJobs_Missing_Notifications]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[mnt_sp_GetJobs_Missing_Notifications]
as

Declare @LocalServer varchar(30)
Declare @sServerIP varchar(30)
Declare @sServerName varchar(50)
Declare @sSQL nvarchar(4000)

SET NOCOUNT ON
 
--Select * from #temp_Servers
--drop table #temp_QueryResults
Create table #temp_QueryResults
	(QR_ID int Identity, ServerName varchar(50), JobName varchar(500), EventLog bit, NotificationOper bit)

  Insert into #temp_QueryResults
 Select @@ServerName as ServerName, [name], notify_level_eventlog, notify_email_operator_id 
	from msdb.dbo.sysjobs with (nolock) where notify_email_operator_id = 0  

	
 
If (Select Count(1) from #temp_QueryResults) > 0 
Begin
	Declare @I int
	Declare @MaxCnt int
	Declare @strServerIP varchar(40)
	Declare @strServerName varchar(50)
	Declare @LastServerName varchar(50)
	Declare @JobName varchar(500)
	Declare @EventLog nvarchar(1)
	Declare @Notification nvarchar(1)
	Declare @Body_Mail varchar(max)
	Declare @sLFCR nchar(1)
	Declare @sTab nchar(1)
	Declare @strSQLExecJob nvarchar(4000)

	SET @sLFCR = CHAR(10)
	SET @sTab = CHAR(9)

	Set @I = 1
	Set @MaxCnt =  (Select Count(1) from #temp_QueryResults)
	While @I <= @MaxCnt
	Begin
		Select @strServerName = ServerName, @JobName = JobName, 
				@EventLog = case when EventLog = 0 then 'N' else 'Y' end,
				@Notification = case when NotificationOper = 0 then 'N' else 'Y' end
		From #temp_QueryResults where QR_ID = @I
		
		If @strServerName = @@ServerName
		Begin
			Set @strSQLExecJob = 'Execute ' + 'msdb.dbo.sp_update_job '
		End
		Else
		Begin
			Set @strSQLExecJob = 'Execute ' + @strServerIP + 'msdb.dbo.sp_update_job '
		End
		
			Set @strSQLExecJob = @strSQLExecJob + '@job_name = ''' + @JobName + ''''
			Set @strSQLExecJob = @strSQLExecJob + ', @notify_level_email = 2, '
			Set @strSQLExecJob = @strSQLExecJob + '@notify_email_operator_name = ''SQLOper'''
			
		Exec sp_ExecuteSQL @strSQLExecJob

		If @I = 1
		Begin
			Set @LastServerName = @strServerName
			Set @Body_Mail = @strServerName + @sLFCR
			Set @Body_Mail = @Body_Mail + @sTab + @sTab + @JobName + @sLFCR
		End
		Else
		Begin
			If @LastServerName = @strServerName
			Begin
				Set @Body_Mail = @Body_Mail + @sTab + @sTab + @JobName + @sLFCR
			End
			Else
			Begin
				Set @Body_Mail = @Body_Mail + @sLFCR + @strServerName + @sLFCR
				Set @Body_Mail = @Body_Mail + @sTab + @sTab + @JobName + @sLFCR
			End
		End
		Set @I = @I + 1
		Set @LastServerName = @strServerName
	End
	Drop table #temp_QueryResults
	Exec msdb..sp_send_dbmail
		@profile_name =  'SQLAlerts',
		@recipients = 'chrisb@sqldatabasesolutions.com;',
		@body = @Body_Mail, 
		@subject = 'Notification was added to the following Jobs',
		@importance ='HIGH'
End
GO
/****** Object:  StoredProcedure [dbo].[mnt_sp_GetSQLEvent]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


CREATE Procedure [dbo].[mnt_sp_GetSQLEvent]
 
 @UserName varchar(250) = NULL
  , @ClientHostName varchar(250) = NULL
  , @Database varchar(250) = NULL
  , @Severity varchar(250) = NULL
  , @ErrMsg varchar(250) = NULL

As


If @UserName IS NOT NULL
BEGIN
	--Count by UserName
	--Select COUNT(*) as EventCount
	--	, dbo.fn_Round5min(ErrorTime) as ErrorTime_Rounded
	--	, ISNULL(UserName, 'NULL') As UserName
	--From [mntblSQLExceptionLog_Detail] with (nolock)
	--Where ErrorTime > DateAdd(HOUR, -8, GETDATE())
	--Group By dbo.fn_Round5min(ErrorTime), UserName
	--Order By 2 Desc
	
		Select ErrorTime, ServerName, ClientHostName
		, CASE WHEN DatabaseID = 0 THEN 'NULL' ELSE DB_NAME(DatabaseID) END as DBName
		, UserName, ErrorSeverity
		, Case 
			When ErrorSeverity = 11 Then 'DB Object Not Found'
			When ErrorSeverity = 14 Then 'Insufficient Permission'
			When ErrorSeverity = 15 Then 'Syntax Error in SQL Stmt'
			When ErrorSeverity = 16 Then 'Misc. User Error'
			When ErrorSeverity = 17 Then 'Insufficient Resources'
			When ErrorSeverity = 18 Then 'Nonfatal Internal Error'
			When ErrorSeverity = 19 Then 'Fatal Error in Resource'
			When ErrorSeverity = 20 Then 'Fatal Error in Current Process'
			When ErrorSeverity = 21 Then 'Fatal Error in Database Processes'
			When ErrorSeverity = 22 Then 'Fatal Error Table Integrity Suspect'
			When ErrorSeverity = 23 Then 'Fatal Error DB Integrity Suspect'
			When ErrorSeverity = 24 Then 'Fatal Error Hardware Error'
			When ErrorSeverity = 25 Then 'Fatal Error'
			Else 'Not Mapped'
		 End as SeverityDescription
		, Error, ErrorMessage, SQLText
	From [mntblSQLExceptionLog_Detail] with (nolock)
	Where ErrorTime > DateAdd(HOUR, -8, GETDATE()) 
	  And ISNULL(UserName, 'NULL') = @UserName
	Order By 1
	
	Return
END

IF @ClientHostName IS NOT NULL
BEGIN
	--Client
	--Select COUNT(*) as EventCount
	--	, dbo.fn_Round5min(ErrorTime) as ErrorTime_Rounded
	--	, ClientHostName
	--From [mntblSQLExceptionLog_Detail] with (nolock)
	--Where ErrorTime > DateAdd(HOUR, -8, GETDATE())
	--Group By dbo.fn_Round5min(ErrorTime), ClientHostName
	--Order By 2 Desc
	
	
		Select ErrorTime, ServerName, ClientHostName
		, CASE WHEN DatabaseID = 0 THEN 'NULL' ELSE DB_NAME(DatabaseID) END as DBName
		, UserName, ErrorSeverity
		, Case 
			When ErrorSeverity = 11 Then 'DB Object Not Found'
			When ErrorSeverity = 14 Then 'Insufficient Permission'
			When ErrorSeverity = 15 Then 'Syntax Error in SQL Stmt'
			When ErrorSeverity = 16 Then 'Misc. User Error'
			When ErrorSeverity = 17 Then 'Insufficient Resources'
			When ErrorSeverity = 18 Then 'Nonfatal Internal Error'
			When ErrorSeverity = 19 Then 'Fatal Error in Resource'
			When ErrorSeverity = 20 Then 'Fatal Error in Current Process'
			When ErrorSeverity = 21 Then 'Fatal Error in Database Processes'
			When ErrorSeverity = 22 Then 'Fatal Error Table Integrity Suspect'
			When ErrorSeverity = 23 Then 'Fatal Error DB Integrity Suspect'
			When ErrorSeverity = 24 Then 'Fatal Error Hardware Error'
			When ErrorSeverity = 25 Then 'Fatal Error'
			Else 'Not Mapped'
		 End as SeverityDescription
		, Error, ErrorMessage, SQLText
	From [mntblSQLExceptionLog_Detail] with (nolock)
	Where ErrorTime > DateAdd(HOUR, -8, GETDATE()) 
	  And ClientHostName = @ClientHostName
	Order By 1
	
	Return
END

IF @Database IS NOT NULL
BEGIN
	--Database
	-- Select COUNT(*) as EventCount
	--	, dbo.fn_Round5min(ErrorTime) as ErrorTime_Rounded
	--	, CASE WHEN DatabaseID = 0 THEN 'NULL' ELSE DB_NAME(DatabaseID) END as DBName
	--From [mntblSQLExceptionLog_Detail] with (nolock)
	--Where ErrorTime > DateAdd(HOUR, -8, GETDATE())
	--Group By dbo.fn_Round5min(ErrorTime), DB_NAME(DatabaseID), DatabaseID
	--Order By 2 Desc
	
	
		Select ErrorTime, ServerName, ClientHostName
		, CASE WHEN DatabaseID = 0 THEN 'NULL' ELSE DB_NAME(DatabaseID) END as DBName
		, UserName, ErrorSeverity
		, Case 
			When ErrorSeverity = 11 Then 'DB Object Not Found'
			When ErrorSeverity = 14 Then 'Insufficient Permission'
			When ErrorSeverity = 15 Then 'Syntax Error in SQL Stmt'
			When ErrorSeverity = 16 Then 'Misc. User Error'
			When ErrorSeverity = 17 Then 'Insufficient Resources'
			When ErrorSeverity = 18 Then 'Nonfatal Internal Error'
			When ErrorSeverity = 19 Then 'Fatal Error in Resource'
			When ErrorSeverity = 20 Then 'Fatal Error in Current Process'
			When ErrorSeverity = 21 Then 'Fatal Error in Database Processes'
			When ErrorSeverity = 22 Then 'Fatal Error Table Integrity Suspect'
			When ErrorSeverity = 23 Then 'Fatal Error DB Integrity Suspect'
			When ErrorSeverity = 24 Then 'Fatal Error Hardware Error'
			When ErrorSeverity = 25 Then 'Fatal Error'
			Else 'Not Mapped'
		 End as SeverityDescription
		, Error, ErrorMessage, SQLText
	From [mntblSQLExceptionLog_Detail] with (nolock)
	Where ErrorTime > DateAdd(HOUR, -8, GETDATE()) 
	  And DB_NAME(DatabaseID) = @Database
	Order By 1
	
	Return
END

IF @Severity IS NOT NULL
BEGIN
	----Severity
	--Select COUNT(*) as EventCount
	--	, dbo.fn_Round5min(ErrorTime) as ErrorTime_Rounded
	--	, Case 
	--		When ErrorSeverity = 11 Then 'DB Object Not Found'
	--		When ErrorSeverity = 14 Then 'Insufficient Permission'
	--		When ErrorSeverity = 15 Then 'Syntax Error in SQL Stmt'
	--		When ErrorSeverity = 16 Then 'Misc. User Error'
	--		When ErrorSeverity = 17 Then 'Insufficient Resources'
	--		When ErrorSeverity = 18 Then 'Nonfatal Internal Error'
	--		When ErrorSeverity = 19 Then 'Fatal Error in Resource'
	--		When ErrorSeverity = 20 Then 'Fatal Error in Current Process'
	--		When ErrorSeverity = 21 Then 'Fatal Error in Database Processes'
	--		When ErrorSeverity = 22 Then 'Fatal Error Table Integrity Suspect'
	--		When ErrorSeverity = 23 Then 'Fatal Error DB Integrity Suspect'
	--		When ErrorSeverity = 24 Then 'Fatal Error Hardware Error'
	--		When ErrorSeverity = 25 Then 'Fatal Error'
	--		Else 'Not Mapped'
	--	 End as Severity
	--From [mntblSQLExceptionLog_Detail] with (nolock)
	--Where ErrorTime > DateAdd(HOUR, -8, GETDATE())
	--Group By dbo.fn_Round5min(ErrorTime), ErrorSeverity
	--Order By 2 Desc
	Select ErrorTime, ServerName, ClientHostName
		, CASE WHEN DatabaseID = 0 THEN 'NULL' ELSE DB_NAME(DatabaseID) END as DBName
		, UserName, ErrorSeverity
		, Case 
			When ErrorSeverity = 11 Then 'DB Object Not Found'
			When ErrorSeverity = 14 Then 'Insufficient Permission'
			When ErrorSeverity = 15 Then 'Syntax Error in SQL Stmt'
			When ErrorSeverity = 16 Then 'Misc. User Error'
			When ErrorSeverity = 17 Then 'Insufficient Resources'
			When ErrorSeverity = 18 Then 'Nonfatal Internal Error'
			When ErrorSeverity = 19 Then 'Fatal Error in Resource'
			When ErrorSeverity = 20 Then 'Fatal Error in Current Process'
			When ErrorSeverity = 21 Then 'Fatal Error in Database Processes'
			When ErrorSeverity = 22 Then 'Fatal Error Table Integrity Suspect'
			When ErrorSeverity = 23 Then 'Fatal Error DB Integrity Suspect'
			When ErrorSeverity = 24 Then 'Fatal Error Hardware Error'
			When ErrorSeverity = 25 Then 'Fatal Error'
			Else 'Not Mapped'
		 End as SeverityDescription
		, Error, ErrorMessage, SQLText
	From [mntblSQLExceptionLog_Detail] with (nolock)
	Where ErrorTime > DateAdd(HOUR, -8, GETDATE()) 
	  And LEFT(ErrorMessage, 25) = @ErrMsg
	Order By 1 
	
	Return
END

IF @ErrMsg IS NOT NULL
BEGIN
	--ErrMsg
	--Select COUNT(*) as EventCount
	--	, dbo.fn_Round5min(ErrorTime) as ErrorTime_Rounded
	--	, LEFT(ErrorMessage, 25) as ErrorMsgShort
	--From [mntblSQLExceptionLog_Detail] with (nolock)
	--Where ErrorTime > DateAdd(HOUR, -8, GETDATE())
	--Group By dbo.fn_Round5min(ErrorTime), LEFT(ErrorMessage, 25)
	--Order By 2 Desc
	
	
		Select ErrorTime, ServerName, ClientHostName
		, CASE WHEN DatabaseID = 0 THEN 'NULL' ELSE DB_NAME(DatabaseID) END as DBName
		, UserName, ErrorSeverity
		, Case 
			When ErrorSeverity = 11 Then 'DB Object Not Found'
			When ErrorSeverity = 14 Then 'Insufficient Permission'
			When ErrorSeverity = 15 Then 'Syntax Error in SQL Stmt'
			When ErrorSeverity = 16 Then 'Misc. User Error'
			When ErrorSeverity = 17 Then 'Insufficient Resources'
			When ErrorSeverity = 18 Then 'Nonfatal Internal Error'
			When ErrorSeverity = 19 Then 'Fatal Error in Resource'
			When ErrorSeverity = 20 Then 'Fatal Error in Current Process'
			When ErrorSeverity = 21 Then 'Fatal Error in Database Processes'
			When ErrorSeverity = 22 Then 'Fatal Error Table Integrity Suspect'
			When ErrorSeverity = 23 Then 'Fatal Error DB Integrity Suspect'
			When ErrorSeverity = 24 Then 'Fatal Error Hardware Error'
			When ErrorSeverity = 25 Then 'Fatal Error'
			Else 'Not Mapped'
		 End as SeverityDescription
		, Error, ErrorMessage, SQLText
	From [mntblSQLExceptionLog_Detail] with (nolock)
	Where ErrorTime > DateAdd(HOUR, -8, GETDATE()) 
	  And LEFT(ErrorMessage, 25) = @ErrMsg
	Order By 1
	
	
	Return
END


--Else
Begin
	--Total
	 
	Select ErrorTime, ServerName, ClientHostName
		, CASE WHEN DatabaseID = 0 THEN 'NULL' ELSE DB_NAME(DatabaseID) END as DBName
		, UserName, ErrorSeverity
		, Case 
			When ErrorSeverity = 11 Then 'DB Object Not Found'
			When ErrorSeverity = 14 Then 'Insufficient Permission'
			When ErrorSeverity = 15 Then 'Syntax Error in SQL Stmt'
			When ErrorSeverity = 16 Then 'Misc. User Error'
			When ErrorSeverity = 17 Then 'Insufficient Resources'
			When ErrorSeverity = 18 Then 'Nonfatal Internal Error'
			When ErrorSeverity = 19 Then 'Fatal Error in Resource'
			When ErrorSeverity = 20 Then 'Fatal Error in Current Process'
			When ErrorSeverity = 21 Then 'Fatal Error in Database Processes'
			When ErrorSeverity = 22 Then 'Fatal Error Table Integrity Suspect'
			When ErrorSeverity = 23 Then 'Fatal Error DB Integrity Suspect'
			When ErrorSeverity = 24 Then 'Fatal Error Hardware Error'
			When ErrorSeverity = 25 Then 'Fatal Error'
			Else 'Not Mapped'
		 End as SeverityDescription
		, Error, ErrorMessage, SQLText
	From [mntblSQLExceptionLog_Detail] with (nolock)
	Where ErrorTime > DateAdd(HOUR, -8, GETDATE()) 
	Order By 1
	
	Return
End
GO
/****** Object:  StoredProcedure [dbo].[mnt_sp_MoveCounterDataToAvg]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [dbo].[mnt_sp_MoveCounterDataToAvg]

AS

DECLARE @purgeDt DATETIME
SET @purgeDt = GETDATE() -2

DELETE FROM mnttblCounterDetails_Avg WHERE CounterDate > @purgeDt
 
 
--8AM to 5PM
INSERT INTO mnttblCounterDetails_Avg (ServerName, CounterName, CounterDate
	, CounterTimeRange, CounterValueAvg)
SELECT cdAll.ServerName, cdAll.CounterName, CONVERT(varchar(25), cdAll.CounterDateTime, 101) As CounterDate
	, '8AM To 5PM' As CounterTimeRange, Avg(cdAll.CounterValue) As CounterValueAvg
FROM mnttblCounterDetails_All cdAll with (nolock)
	LEFT JOIN mnttblCounterDetails_Avg cdAvg with (nolock) ON cdAll.ServerName = cdavg.ServerName 
											And cdAll.CounterName = cdAvg.CounterName
											And CAST(CONVERT(varchar(25), cdAll.CounterDateTime, 101) AS DATETIME) = cdAvg.CounterDate
											And cdAvg.CounterTimeRange = '8AM To 5PM'
WHERE  DatePart(Hour, CounterDateTime) > 7 and DatePart(Hour, CounterDateTime) < 18
	And cdAvg.serverName IS NULL
	And cdAvg.CounterName IS NULL
	And cdAvg.CounterDate IS NULL
GROUP BY cdAll.ServerName, cdAll.CounterName, CONVERT(varchar(25), cdAll.CounterDateTime, 101) 
ORDER BY 1, 3, 2
GO
/****** Object:  StoredProcedure [dbo].[mnt_sp_ProcessErrorCapture]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE Procedure [dbo].[mnt_sp_ProcessErrorCapture]
as

Declare @ReportID Int
Declare @MinErrTime DateTime
Declare @MaxErrTime DateTime

Create table #Report (fReportID INT, [ErrorTime] [datetime] NOT NULL,	[ServerName] [nvarchar](100) NOT NULL,	[ClientHostName] [nvarchar](100) NULL,	[DatabaseID] [int] NULL,
	[UserName] [nvarchar](100) NULL,	[ErrorSeverity] [bigint] NULL,	[ErrorNumber] [bigint] NULL,	[Error] [nvarchar](512) NULL,	[ErrorMessage] [nvarchar](512) NULL,	[SQLText] [nvarchar](max) NULL,
	[EventData] [xml] NULL) 
	

--Declare @LastProcessTime DateTime
Insert into mntblSQLExceptionLog_Header
	(fEnvironment, fCreated, fDateTimeBeg, fDatetimeEnd )
Select 'Development',GetDate(),Getdate(),GetDate()

Select @ReportID = Max(fReportID) from mntblSQLExceptionLog_Header with (nolock)


--Select @LastProcessTime = Convert(dateTime,Value) from [montblErrorCaptureSetting]

;with events_cte as(
select 
	DATEADD(mi,
	DATEDIFF(mi, GETUTCDATE(), CURRENT_TIMESTAMP),
	xevents.event_data.value('(event/@timestamp)[1]', 'datetime2')) AS [err_timestamp],
	--xevents.event_data.value('(event/action[@name="server_instance_name"]/value)[1]', 'nvarchar(100)') AS [server_instance_name],  Not avialable in SQL 2008
	Substring(file_name,CHARINDEX('\',file_name,5) + 1 ,CHARINDEX('_', file_name,5) - (CHARINDEX('\',file_name,5)+1)) as [server_instance_name],
	xevents.event_data.value('(event/action[@name="client_hostname"]/value)[1]', 'nvarchar(100)') AS [client_hostname],
	xevents.event_data.value('(event/action[@name="database_id"]/value)[1]', 'int') AS [database_id],
	xevents.event_data.value('(event/action[@name="username"]/value)[1]', 'nvarchar(100)') AS [username],
	xevents.event_data.value('(event/data[@name="severity"]/value)[1]', 'bigint') AS [err_severity],
	xevents.event_data.value('(event/data[@name="error_number"]/value)[1]', 'bigint') AS [err_number],
	xevents.event_data.value('(event/data[@name="error"]/value)[1]', 'nvarchar(512)') AS [Error],
	xevents.event_data.value('(event/data[@name="message"]/value)[1]', 'nvarchar(512)') AS [err_message],
	xevents.event_data.value('(event/action[@name="sql_text"]/value)[1]', 'nvarchar(max)') AS [sql_text],
	xevents.event_data
from sys.fn_xe_file_target_read_file
	('C:\MSSQL\Events\*.xel', 
	'C:\MSSQL\Events\*.xem',
	null, null)
cross apply (select CAST(event_data as XML) as event_data) as xevents
)

INSERT INTO #Report (ErrorTime, ServerName,ClientHostName, DatabaseID, UserName,
				 ErrorSeverity, ErrorNumber,Error, ErrorMessage, SQLText,[EventData]
 )
SELECT [err_timestamp],[server_instance_name],[client_hostname],[database_id],[username],
			[err_severity],[err_number],[Error],[err_message],[sql_text],event_data
	
--from events_cte e left outer join #Errors t on e.server_instance_name = t.ServerName and e.err_timestamp > t.MaxErrorTime
from events_cte e where err_message is not null
order by err_timestamp;

Select @MinErrTime = MIN (ErrorTime) ,@MaxErrTime =MAX(ErrorTime) from #Report

Update mntblSQLExceptionLog_Header
Set fDateTimeBeg = @MinErrTime, fDatetimeEnd= @MaxErrTime
Where fReportID = @ReportID

Insert into [dbo].[mntblSQLExceptionLog_Detail] (fReportID, ErrorTime, ServerName, ClientHostName, DatabaseID, UserName, ErrorSeverity, ErrorNumber, Error, ErrorMessage, SQLText, EventData)
Select @ReportID,ErrorTime, ServerName,ClientHostName, DatabaseID, UserName,ErrorSeverity, ErrorNumber,Error, ErrorMessage, SQLText,[EventData]
from #Report

GO
/****** Object:  StoredProcedure [dbo].[mnt_sp_Purge_mnttblCounterDetails_All]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[mnt_sp_Purge_mnttblCounterDetails_All]

as

declare @purgeDt varchar(25), @dYear varchar(5), @dMonth varchar(5), @dDay varchar(5), @dDate datetime

set @dDate = getdate() -15

set @dYear = cast((datepart(year, @dDate)) as varchar(5))
set @dMonth =cast( ( datepart(month, @dDate))  as varchar(5))
set @dDay = cast((datepart(day, @dDate)) as varchar(5))

select @dYear, @dMOnth, @dDay 


if @dMonth = 0
begin
  set @dMonth = 12
  set @dYear = cast(cast(@dYear as int) -1 as int)
end

if len(@dMonth) = 1
begin
  set @dMonth = '0' + @dMonth
end

if len(@dDay) = 1
begin
  set @dDay = '01' + @dDay
end
 
set @purgeDt = @dYear + '-' + @dMonth + '-' + @dDay + ' 00:00:00.000'
-- 
select @purgeDt

delete from mnttblCounterDetails_All where CounterDateTime <  @purgeDt
GO
/****** Object:  StoredProcedure [dbo].[mnt_sp_PurgeCounterData]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[mnt_sp_PurgeCounterData]  
  
as  
  
declare @purgeDt varchar(25), @dYear varchar(5), @dMonth varchar(5), @dDay varchar(5)  
declare @dt datetime  
  
select @dt = GETDATE() -15  
  
set @dYear = cast(datepart(year, @dt) as varchar(5))  
set @dMonth =cast(  datepart(month, @dt)  as varchar(5))  
set @dDay = cast(datepart(day, @dt) as varchar(5))  
   
if len(@dMonth) = 1  
begin  
  set @dMonth = '0' + @dMonth  
end  
  
if len(@dDay) = 1  
begin  
  set @dDay = '01' + @dDay  
end  
   
set @purgeDt = @dYear + '-' + @dMonth + '-' + @dDay + ' 00:00:00.000'  
--   
select @purgeDt  
   
  
DELETE FROM counterdata WHERE CounterDateTime <  @purgeDt
GO
/****** Object:  StoredProcedure [dbo].[mnt_sp_PurgeExceptionTables]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[mnt_sp_PurgeExceptionTables]
AS

SELECT Distinct fReportID 
INTO #Header
FROM mntblSQLExceptionLog_Detail with (nolock)
WHERE ErrorTime < GETDATE() -30

Declare @c Int = 0

CREATE TABLE #ToDel (ItemID INT)

WHILE @c >= 0
BEGIN

	Truncate Table #ToDel

	Insert Into #ToDel (ItemID)
	Select TOP (5000) ItemID 
	From #Header h
		Join mntblSQLExceptionLog_Detail d with (nolock) on h.fReportID = d.fReportID

	Delete d
	From #ToDel t 
		Join mntblSQLExceptionLog_Detail d On t.ItemID = d.ItemID 

	Select @c = @@ROWCOUNT
	If @c = 0
	BEGIN
		GOTO PROC_EXIT
	END


END

PROC_EXIT:

DELETE h
FROM mntblSQLExceptionLog_Header h
   JOIN #Header t on h.fReportID = t.fReportID

DROP TABLE #ToDel
DROP TABLE #Header

 
GO
/****** Object:  StoredProcedure [dbo].[mnt_sp_RptSQLUptime]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create proc [dbo].[mnt_sp_RptSQLUptime]

As

select fSQLServerName As SQLServerName, fDBCreateDate As RunningSince
	, DateDiff(day, fDBCreateDate, getdate()) As UpTimeDays  
from maintenance.dbo.mnttblBackupInfo
where fDatabaseName = 'tempdb'
Order by SQLServerName
GO
/****** Object:  StoredProcedure [dbo].[mntspCheckCounters]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[mntspCheckCounters]

as

declare @sCounterDT varchar(100), @CounterDT datetime

select top 1 @sCounterDT = left(CounterDateTime, 19) from counterdata 
order by CounterDatetime desc

select @CounterDT = cast(@sCounterDT as datetime)

--select @CounterDT
--select getdate()
--select datediff(n, @CounterDT, getdate())

if datediff(n, @CounterDT, getdate()) > 5
begin

	declare @subject nvarchar(100), @msg nvarchar(100)
	set @subject = @@SERVERNAME
	set @msg = 'Last Run: ' + @sCounterDT

	select @subject = 'Restart Counters On: ' + @subject

	exec  msdb..sp_send_dbmail  
		@profile_name =  'SQLAlerts'
		, @recipients  = 'chrisb@sqldatabasesolutions.com' 
		, @body = @msg
		, @subject = @subject
		, @importance ='HIGH'  

end
GO
/****** Object:  StoredProcedure [dbo].[mon_sp_GetWinEventLogs]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[mon_sp_GetWinEventLogs] 
--(
--	--@DateBeg datetime = NULL 
--	)
as

declare @DateBeg datetime = cast(convert(varchar(100), getdate() -14, 101) as datetime)

--if @DateBeg IS NULL or @DateBeg = '1/1/1900'
--begin
--  set @DateBeg = cast(convert(varchar(100), getdate() -14, 101) as datetime)
--end

declare @DateEnd datetime
 
Set @dateEnd = dateadd(d, 1, @DateBeg)

--SELECT @DateBeg, @Dateend

select * from WinEventLogs with (nolock)
where GenDateTime > @DateBeg --and @DateEnd
  --and Message not like 'Login failed for user%'
  and SourceName not in ('Print', 'TermServDevices', 'Microsoft-Windows-TerminalServices-Printers')
  and LogFile not in ('SmartConnect', 'eConnect')
order by ComputerName, Type, GenDateTime desc
GO
/****** Object:  StoredProcedure [dbo].[mon_sp_rptGetSQLActivity]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[mon_sp_rptGetSQLActivity]
	@BegDate datetime = NULL,
	@EndDate datetime = NULL,
	@sortBy tinyint = 0,
	@SQLServerName nvarchar(100)
 AS
  
Declare @BegDateUTC datetime,
	@EndDateUTC datetime , @SubscrRun bit = 0

-- Exec mon_sp_rptGetSQLActivity NULL, NULL, 0, 'LASQLMAINDB'

  --set @BegDate  = '9/24/2018'
  --set @EndDate = '9/28/2018'
--if @sortBy = 0 --Tot_cpu_time / default 
--if @sortBy = 1 -- Avg_cpu_time 
--if @sortBy = 2 -- Tot_logical_reads 
--if @sortBy = 3 -- Avg_logical_reads 
--if @sortBy = 4 -- Tot_Writes 
--if @sortBy = 5 -- Avg_Writes 
--if @sortBy = 6 -- ExecCount
SET NOCOUNT ON;
 
If @BegDate Is NULL AND @EndDate is null
Begin
	Set @BegDate = Cast(GetDate()-1 As Date)
	Set @EndDate =  Cast(GetDate()-1 As Date)
	Set @EndDate = DateAdd(Second, -1, DateAdd(day, 1, @EndDate))
	Set @SubscrRun = 1
End
--Else
--Begin
--	Set @EndDate = DateAdd(Second, -1, DateAdd(day, 1, @EndDate))
--End

--CONVERT INPUT PARAMS TO UTC
--@BegDate & @EndDate come in as PDT
--  1st convert @EndDate to DateAdd+1 -1 second to get 23:59:59.000
-- 2 convert to UTC
SET @BegDateUTC = DATEADD(ss,-1 *DATEDIFF(ss,GETUTCDATE(),GETDATE()),@BegDate) 
SET @EndDateUTC = DATEADD(ss,-1 *DATEDIFF(ss,GETUTCDATE(),GETDATE()),@EndDate) 

--select @BegDate, @EndDate
--select @BegDateUTC, @EndDateUTC

--Get all activity between start/end window
select ConsolId, XEID, SqlServerName, timestamp, pdtlocaltime, groupby_val, item_count, event_name, cpu_time, duration, physical_reads, logical_reads
	, writes, object_name, statement, username, session_id, query_hash, nt_username, database_name, database_id, client_hostname, sql_text 
into #tmp
from montblXE_CPUReadsFilterTrace_Consol with (Nolock)
where pdtlocaltime > @BegDate
	and pdtlocaltime <= @EndDate
	and SqlServerName = @SQLServerName

CREATE TABLE #Final (
	[item_count] [int] NULL,
	[groupby_val] [nvarchar](100) NULL,
	[pdtlocaltime] [datetime] NULL,
	[timestamp] [datetime] NULL,
	[event_name] [nvarchar](50) NULL,
	[cpu_time] [decimal](20, 0) NULL,
	[duration] [decimal](20, 0) NULL,
	[physical_reads] [decimal](20, 0) NULL,
	[logical_reads] [decimal](20, 0) NULL,
	[writes] [decimal](20, 0) NULL,
	[username] [nvarchar](100) NULL,
	[database_name] [nvarchar](100) NULL,
	[client_hostname] [nvarchar](500) NULL
	)


--US Hours only
IF @SubscrRun = 1
BEGIN
	INSERT INTO #Final (Item_Count
		, groupby_val
		, pdtlocaltime, [timestamp], event_name, cpu_time, duration, physical_reads, logical_reads, writes
		, username, database_name, client_hostname )
	SELECT Item_Count
		, groupby_val
		, pdtlocaltime, [timestamp], event_name, cpu_time, duration, physical_reads, logical_reads, writes
		, username, database_name, client_hostname 
	FROM #tmp
	Where Datepart(hour, pdtlocaltime) between 8 and 20
END
ELSE 
BEGIN
	INSERT INTO #Final (Item_Count
		, groupby_val
		, pdtlocaltime, [timestamp], event_name, cpu_time, duration, physical_reads, logical_reads, writes
		, username, database_name, client_hostname )
	SELECT Item_Count
		, groupby_val
		, pdtlocaltime, [timestamp], event_name, cpu_time, duration, physical_reads, logical_reads, writes
		, username, database_name, client_hostname 
	FROM #tmp
END

Select Sum(Item_Count) As ExecCount, GroupBy_Val, event_name
	, Sum(cpu_time) as Tot_cpu_time, Avg(cpu_time) as Avg_cpu_time
	, Sum(logical_reads) As Tot_logical_reads, Avg(logical_reads) As Avg_logical_reads
	, Sum(writes) As Tot_Writes, Avg(writes) As Avg_Writes
	, Sum(duration) As Tot_Duration, Avg(duration) As Avg_Duration
Into #Return
From #Final
Group by groupby_val, event_name

 
if @sortBy = 0 --Tot_cpu_time / default
begin
	Select Top 15 ExecCount, groupby_val, event_name, Tot_cpu_time, Tot_cpu_time / ExecCount as Avg_cpu_time
		, Tot_logical_reads, Tot_logical_reads / ExecCount as Avg_logical_reads, Tot_Writes, Tot_Writes / ExecCount as Avg_Writes 
		, Tot_Duration, Tot_Duration / ExecCount as Avg_Duration
	From #Return
	Order by Tot_cpu_time desc
end

if @sortBy = 1 -- Avg_cpu_time
begin
	Select Top 15 ExecCount, groupby_val, event_name, Tot_cpu_time, Tot_cpu_time / ExecCount as Avg_cpu_time
		, Tot_logical_reads, Tot_logical_reads / ExecCount as Avg_logical_reads, Tot_Writes, Tot_Writes / ExecCount as Avg_Writes 
		, Tot_Duration, Tot_Duration / ExecCount as Avg_Duration
	From #Return
	Order by Avg_cpu_time desc
end

if @sortBy = 2 -- Tot_logical_reads
begin
	Select Top 15 ExecCount, groupby_val, event_name, Tot_cpu_time, Tot_cpu_time / ExecCount as Avg_cpu_time
		, Tot_logical_reads, Tot_logical_reads / ExecCount as Avg_logical_reads, Tot_Writes, Tot_Writes / ExecCount as Avg_Writes 
		, Tot_Duration, Tot_Duration / ExecCount as Avg_Duration
	From #Return
	Order by Tot_logical_reads desc
end

if @sortBy = 3 -- Avg_logical_reads
begin
	Select Top 15 ExecCount, groupby_val, event_name, Tot_cpu_time, Tot_cpu_time / ExecCount as Avg_cpu_time
		, Tot_logical_reads, Tot_logical_reads / ExecCount as Avg_logical_reads, Tot_Writes, Tot_Writes / ExecCount as Avg_Writes
		, Tot_Duration, Tot_Duration / ExecCount as Avg_Duration 
	From #Return
	Order by Avg_logical_reads desc
end

if @sortBy = 4 -- Tot_Writes
begin
	Select Top 15 ExecCount, groupby_val, event_name, Tot_cpu_time, Tot_cpu_time / ExecCount as Avg_cpu_time
		, Tot_logical_reads, Tot_logical_reads / ExecCount as Avg_logical_reads, Tot_Writes, Tot_Writes / ExecCount as Avg_Writes 
		, Tot_Duration, Tot_Duration / ExecCount as Avg_Duration
	From #Return
	Order by Tot_Writes desc
end

if @sortBy = 5 -- Avg_Writes
begin
	Select Top 15 ExecCount, groupby_val, event_name, Tot_cpu_time, Tot_cpu_time / ExecCount as Avg_cpu_time
		, Tot_logical_reads, Tot_logical_reads / ExecCount as Avg_logical_reads, Tot_Writes, Tot_Writes / ExecCount as Avg_Writes 
		, Tot_Duration, Tot_Duration / ExecCount as Avg_Duration
	From #Return
	Order by Avg_Writes desc
end

if @sortBy = 6 -- ExecCount
begin
	Select Top 15 ExecCount, groupby_val, event_name, Tot_cpu_time, Tot_cpu_time / ExecCount as Avg_cpu_time
		, Tot_logical_reads, Tot_logical_reads / ExecCount as Avg_logical_reads, Tot_Writes, Tot_Writes / ExecCount as Avg_Writes 
		, Tot_Duration, Tot_Duration / ExecCount as Avg_Duration
	From #Return
	Order by ExecCount desc
end

Drop table #Final
Drop table #tmp
Drop table #Return
GO
/****** Object:  StoredProcedure [dbo].[mon_sp_rptGetSQLActivity_StmtDetails]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[mon_sp_rptGetSQLActivity_StmtDetails]
	@BegDate datetime = NULL,
	@EndDate datetime = NULL,
	@SqlServerName nvarchar(50),
	@groupby_val nvarchar(250)
 AS

 /* 
 --Purpose: Drives drill-through subreport for XE rpc & stmt_completed trace
 --Author:  Chris Becker
 --Created: 10/3/2018
 -- 10/16/2018	Added pdtlocaltime_rounded
 Exec mon_sp_rptGetSQLActivity_StmtDetails @BegDate=N'2018-10-09 20:30:58.880', @EndDate=N'2018-10-09 20:50:58.880', @SqlServerName = 'LASQLMAINDB', @groupby_val = 'ts_sp_MEolOrderRecd'
 
 */
  
 
SET NOCOUNT ON;
 
----If null set to yesterday 12am-11.59pm
--If @BegDate Is NULL 
--Begin
--	Set @BegDate = Cast(GetDate()-1 As Date)
--	Set @EndDate = DateAdd(second, -1, cast(Cast(GetDate() As Date) as datetime))
--End
--Else
--Begin
--	Set @EndDate = DateAdd(Second, -1, DateAdd(day, 1, @EndDate))
--End

--CONVERT INPUT PARAMS TO UTC
--@BegDate & @EndDate come in as PDT
--  1st convert @EndDate to DateAdd+1 -1 second to get 23:59:59.000
-- 2 convert to UTC
--SET @BegDateUTC = DATEADD(ss,-1 *DATEDIFF(ss,GETUTCDATE(),GETDATE()),@BegDate) 
--SET @EndDateUTC = DATEADD(ss,-1 *DATEDIFF(ss,GETUTCDATE(),GETDATE()),@EndDate) 
--	***don't need utc, have ssis pop pdtlocaltime on way over. plus cleansed groupby_val

--select @BegDate, @EndDate
--select @BegDateUTC, @EndDateUTC
 

--Get all activity between start/end window
select item_count, groupby_val, SqlServerName, [timestamp] as timestamp_utc, [statement]
	, pdtlocaltime, Maintenance.dbo.fn_Round5min(pdtlocaltime) as pdtlocaltime_rounded, event_name, cpu_time, duration, logical_reads, writes
	, username, database_name, client_hostname 
from montblXE_CPUReadsFilterTrace_Consol with (Nolock)
where pdtlocaltime > @BegDate 
	and pdtlocaltime <= @EndDate 
	and SqlServerName = @SqlServerName
	and groupby_val = @groupby_val
Order by pdtlocaltime

--where DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()), [timestamp]) > @BegDate --dateadd(hour, -32, getutcdate())
--	and DATEADD(mi, DATEDIFF(mi, GETUTCDATE(), GETDATE()), [timestamp]) < @EndDate 

----US Hours only
--SELECT ItemCount
--	, GroupByVal
--	, pdtlocaltime, [timestamp], event_name, cpu_time, duration, physical_reads, logical_reads, writes
--	, username, database_name, client_hostname
--Into #Final
--FROM #tmp
--Where Datepart(hour, pdtlocaltime) between 8 and 18 --8am - 6pm
GO
/****** Object:  StoredProcedure [dbo].[mon_sp_WinEventAlert]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

create proc [dbo].[mon_sp_WinEventAlert]

as

declare @DateBeg datetime , @TimeGenerated datetime
Set @DateBeg = dateadd(d, -1, getdate())

if exists(select top 1 * from WinEventLogs with (nolock)
	where GenDateTime > @DateBeg --and @DateEnd
	  and Message like 'The Alder Process Scheduler service terminated unexpectedly%'
	)
begin
	select Top 1 @TimeGenerated = GenDateTime from WinEventLogs with (nolock)
	where GenDateTime > @DateBeg --and @DateEnd
	  and Message like 'The Alder Process Scheduler service terminated unexpectedly%'
 
	declare @subject nvarchar(100)
	set @subject = @@SERVERNAME 
	select @subject = 'The Alder Process Scheduler service terminated unexpectedly on VSQLSERVER: ' + cast(@TimeGenerated as varchar(100))
	
	exec  msdb..sp_send_dbmail  
		@profile_name =  'SQLAlerts'
		, @recipients  = 'chris.b@alder.com' 
		, @body = @subject
		, @subject = 'The Alder Process Scheduler service terminated unexpectedly on VSQLSERVER' --@subject
		, @importance ='HIGH'  

end 
GO
/****** Object:  StoredProcedure [dbo].[proc_mnt_DefragTables]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
 
/** Performs on-line index rebuilds **/

CREATE PROCEDURE [dbo].[proc_mnt_DefragTables] 

AS 

/*

ModifiedBy	ModifiedDate	Comments
---------	----------	-----------------------------
ChrisB		1/17/06		Birthday. Copied from job_sp_tmpDefragTables.
						Now using systblTables, also logging progress
						to Maintenance..mnttblTableDefagMaint
ChrisB		8/26/07		Updated to 2008 Syntax
ChrisB		12/24/08    Use DMV/DMF and Check Edition
ChrisB		9/14/09		text, ntext etc must be offline

*/

-- Declare variables
SET NOCOUNT ON
DECLARE @tablename		VARCHAR (128)
DECLARE @tableschema		VARCHAR (128)
DECLARE @LastTableName	VARCHAR (128)
DECLARE @execstr		VARCHAR (355)
DECLARE @objectid  		INT
DECLARE @indexid   		INT
DECLARE @frag_Percent  	DECIMAL
DECLARE @IndexName  	VARCHAR(255)
DECLARE @Edition  		VARCHAR(255)
DECLARE @maxfrag   		DECIMAL
DECLARE @dfrag  		DECIMAL			-- Determine ReOrganize or Rebuild Index
DECLARE @bDefrag   		BIT
DECLARE @JobStartDate	DATETIME
DECLARE @ssubject		VARCHAR(100)
DECLARE @Msg_Body		NVARCHAR(max)
DECLARE @ServerName		NVarchar(20)
DECLARE @DBName			NVarchar(50)
DECLARE @Row_Count		DECIMAL
DECLARE @dRow_Count		DECIMAL			-- Determine ReOrganize or Rebuild Index based on rowcount
DECLARE @IsOffline		BIT

SELECT 	@JobStartDate = GETDATE()
SELECT 	@Edition = left(Convert(Varchar,(ServerProperty('Edition'))), 18)
SELECT  @ServerName = @@SERVERNAME
SELECT  @DBName  = db_name()

-- Decide on the maximum fragmentation to allow
SELECT @maxfrag = 10
SELECT @bDefrag = 1
SELECT @dfrag = 30
SELECT @dRow_Count	= 1000000


SET	@Msg_Body = ''
SET @ssubject = @ServerName +'-'+ LTrim(RTrim(@DBName)) + ' Index Fragmentation more than ' +  Convert (varchar,@dfrag) + 'Percent'



-- Create the table
CREATE TABLE #fraglist (ObjectName CHAR (255), Table_Schema varchar(100), IndexId INT, IndexName CHAR (255), avg_fragmentation_in_percent decimal(18,2),
			Row_Count Decimal (18,0), IsOffline BIT)

CREATE TABLE #Tables (Table_Name Varchar(100), Table_Schema varchar(100))

INSERT INTO #Tables ( Table_Name, Table_Schema)	
SELECT table_name, TABLE_SCHEMA
FROM information_schema.tables i
 --INNER JOIN systblTables t ON i.table_name = t.fTablename 
WHERE i.table_type = 'BASE TABLE'
  And i.table_name not like '%DELETE%'
  and i.table_name not in ('sysarticlecolumns',
	'sysarticleupdates',
	'sysdiagrams',
	'syspublications',
	'sysreplservers',
	'sysschemaarticles',
	'syssubscriptions')
ORDER BY 1
   

CREATE TABLE #OfflineTables (Table_Name varchar(100))

INSERT INTO #OfflineTables
SELECT distinct table_name  
FROM information_schema.columns 
WHERE data_type in ('text', 'ntext', 'image', 'varchar(max)', 'nvarchar(max)', 'varbinary(max)')
UNION
SELECT distinct table_name 
FROM information_schema.columns 
WHERE data_type in ('varchar', 'nvarchar', 'varbinary')
  And character_maximum_length = -1
 

-- Use DMF to determine frangmentation percent
INSERT INTO #fraglist ( ObjectName, Table_Schema, IndexId, IndexName, avg_fragmentation_in_percent, IsOffline)
SELECT Object_Name(dt.Object_id), t.Table_Schema, si.index_id, si.name, dt.avg_fragmentation_in_percent, 0
FROM
	(select object_id, index_id, avg_fragmentation_in_percent
	 FROM sys.dm_db_index_physical_stats(db_id(),NULL,NULL,NULL,'Limited')	
	 Where Index_id <> 0) as dt
 INNER JOIN sys.Indexes as si
		on si.object_id = dt.Object_id and si.Index_id = dt.Index_id
 INNER JOIN #Tables t on t.Table_Name = Object_Name(dt.Object_id)	
  
-- Update Approx RowCount
Update t
Set t.Row_Count = i.rows
from #fraglist t join sysindexes i on t.ObjectName = convert(varchar(128),object_name(i.id))
Where indid = 1 

--Tables to be built offline due to datatype
UPDATE f SET IsOffline = 1
FROM #fraglist f
  JOIN #OfflineTables o on f.ObjectName = o.table_name
 
-- Declare cursor for list of indexes to be defragged
DECLARE indexes CURSOR FOR
   SELECT ObjectName as TableName, Table_Schema as TableSchema, IndexId, avg_fragmentation_in_percent,
   		IndexName, Row_Count, IsOffline
   FROM #fraglist
   WHERE avg_fragmentation_in_percent >= @maxfrag
   ORDER BY 1
      

-- Open the cursor
OPEN indexes

-- loop through the indexes
FETCH NEXT
   FROM indexes
   INTO @tablename, @tableschema, @indexid, @frag_Percent, @IndexName, @Row_Count, @IsOffline

WHILE @@FETCH_STATUS = 0
BEGIN

   INSERT INTO Maintenance.dbo.mnttblTableDefagMaint (fServerName, fDatabaseName, fTableName, fIndexName, fFragPercent, fJobStartDate
	, fDefragStartTime)
   SELECT @@SERVERNAME, db_name(), @tablename, @IndexName, @frag_Percent, @JobStartDate, GETDATE()

   -- SQL2005 Syntax- ONLINE only for Enterprise Edition
   --  ALTER INDEX IX_lttblBeneficiaries_fCountry ON lttblBeneficiaries
   --   REBUILD WITH (FILLFACTOR = 80, ONLINE = ON);

   --DEPRECATED SQL2000 Syntax
   --SELECT @execstr = 'DBCC INDEXDEFRAG (0, ' + RTRIM(@objectid) + ', ' + RTRIM(@indexid) + ') WITH NO_INFOMSGS '
	SELECT @execstr  = 	
	   Case 
		When @IsOffline = 1 THEN
			'ALTER INDEX [' + rtrim(@IndexName) + '] ON [' + rtrim(@tableschema) + '].[' + rtrim(@tablename) + '] REBUILD WITH (FILLFACTOR = 80)'
 		When LTrim(RTrim(@Edition)) = 'Enterprise Edition' THEN
  			 'ALTER INDEX [' + rtrim(@IndexName) + '] ON [' + rtrim(@tableschema) + '].[' + rtrim(@tablename) + '] REBUILD WITH (FILLFACTOR = 80, ONLINE = ON)'
		When LTrim(RTrim(@Edition)) <> 'Enterprise Edition' and @frag_Percent < @dfrag THEN
			'ALTER INDEX [' + rtrim(@IndexName) + '] ON [' + rtrim(@tableschema) + '].[' + rtrim(@tablename) + '] REORGANIZE  WITH (LOB_COMPACTION = ON)'
		When LTrim(RTrim(@Edition)) <> 'Enterprise Edition' and @frag_Percent >= @dfrag --and @Row_Count < @dRow_Count 
			THEN
			'ALTER INDEX [' + rtrim(@IndexName) + '] ON [' + rtrim(@tableschema) + '].[' + rtrim(@tablename) + '] REBUILD WITH (FILLFACTOR = 80)'
		Else 'ALTER INDEX [' + rtrim(@IndexName) + '] ON [' + rtrim(@tableschema) + '].[' + rtrim(@tablename) + '] REBUILD WITH (FILLFACTOR = 80)'
	   End	   		

    --Print @execstr 
     EXEC (@execstr)



   -- If Edition not Enterprise and fragmentation >=  @dfrag send alert
	If LTrim(RTrim(@Edition)) <> 'Enterprise Edition' and @frag_Percent >= @dfrag and @Row_Count >= @dRow_Count
	 Begin
		SET @Msg_Body = @Msg_Body + Rtrim(@TableName) + ' ' + Rtrim(@IndexName) + '    ' +  Convert(Varchar,@frag_Percent) + '    ' +  Convert(Varchar,@Row_Count)
		SET @Msg_Body = @Msg_Body + CHAR(13)
	 End
	
   UPDATE Maintenance.dbo.mnttblTableDefagMaint SET fDefragEndTime = GETDATE()
   WHERE fIndexName = @IndexName
     And fJobStartDate = @JobStartDate

   SET @LastTableName = @tablename
   
   FETCH NEXT
      FROM indexes
      INTO @tablename, @tableschema, @indexid, @frag_Percent, @IndexName, @Row_Count, @IsOffline

--   
   IF (@@FETCH_STATUS <> 0  OR @LastTableName <> @tableName)  BEGIN

      	UPDATE Maintenance.dbo.mnttblTableDefagMaint SET fUpdateStatsStartTime = GETDATE()
      	WHERE fTableName = @LastTableName
            And fJobStartDate = @JobStartDate

			--SELECT @execstr = 'UPDATE STATISTICS [' + @tableschema + '].[' + @LastTableName + ']'
			--EXEC (@execstr)
			--print @execstr

      	UPDATE Maintenance.dbo.mnttblTableDefagMaint SET fUpdateStatsEndTime = GETDATE()
      	WHERE fTableName = @LastTableName
			  And fJobStartDate = @JobStartDate

   END
  
END
 

-- Close and deallocate the cursor
CLOSE indexes
DEALLOCATE indexes

-- Delete the temporary table
DROP TABLE #fraglist
DROP TABLE #Tables

DROP TABLE #OfflineTables









GO
/****** Object:  StoredProcedure [dbo].[spBlockCheck]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[spBlockCheck]

as

select spid, blocked
into #blocks
from master.dbo.sysprocesses (nolock)
where blocked <> 0
  and blocked <> spid

insert into blocking_spid (spid, s.blocked, s.waittime, s.cmd, s.hostname, s.program_name, s.loginame, block_datetime)
select s.spid, s.blocked, s.waittime, s.cmd, s.hostname, s.program_name, s.loginame, getdate()
from master.dbo.sysprocesses s (nolock) 
  inner join (SELECT b.blocked 
		FROM #blocks b
		  LEFT JOIN #blocks s2 ON b.blocked = s2.spid
		WHERE s2.spid IS NULL) t on s.spid = t.blocked
  left join blocking_spid bl on s.spid = bl.spid and s.hostname = bl.hostname
where bl.spid is null
  and bl.hostname is null

if exists( select top 1 * from blocking_spid )
begin
  if exists(select top 1 * from blocking_spid where block_datetime < dateadd(n, -2, getdate()))
  begin
    declare @message nvarchar(2000), @subject nvarchar(2000)

    set @message = 'Block on ' + @@servername
	select @subject = 'Host:' + hostname from blocking_spid 

	 exec  msdb..sp_send_dbmail  
	 @profile_name = 'SQLAlerts'  
	 ,@recipients = 'chrisb@sqldatabasesolutions.com'  
	 ,@body = @message
	 ,@subject =  @subject
	 ,@importance ='HIGH' 

     truncate table blocking_spid

  end
end

drop table #blocks


/*
select  dateadd(n, -2, getdate())
select getdate()
select * from blocking_spid

*/
GO
/****** Object:  StoredProcedure [dbo].[spBlockingAlert]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[spBlockingAlert]

as

/*

By	Date	Comments
---	----	------------------
ChrisB	05/30/06	Birthday. Sends alert when spids block longer than 2 minutes
 

*/

create table #tblBlockingSpids (spid int, cmd nvarchar(500), hostname nvarchar(100)
	, program_name nvarchar(500), loginame nvarchar(500), timelogged datetime)

create table #blocks (spid int, blocked int)

insert into #blocks (spid, blocked)
select spid, blocked
from master.dbo.sysprocesses (nolock)
where blocked <> 0
  and blocked <> spid


-- blocking spid  
insert into #tblBlockingSpids (spid, cmd, hostname, program_name, loginame, timelogged )
select s.spid, s.cmd, s.hostname, s.program_name, s.loginame, getdate()
from master.dbo.sysprocesses s (nolock) 
  inner join (SELECT b.blocked 
		FROM #blocks b
		  LEFT JOIN #blocks s2 ON b.blocked = s2.spid
		WHERE s2.spid IS NULL) t on s.spid = t.blocked

-- If nothing blocked, clear table
if not exists(select spid from #tblBlockingSpids)
  truncate table Maintenance.dbo.tblBlockingSpids

-- Else, spids blocked


-- longer than 1 minute(s)?, check for last 10 minutes of history only
if exists ( 
   select t.spid, m.timelogged
   from #tblBlockingSpids t
     join Maintenance.dbo.tblBlockingSpids m on t.spid = m.spid 
   where m.timelogged between dateadd(mi, -10, getdate()) and dateadd(ss, -10, getdate())  )
begin

  --print 'send alert'
  --return

   declare @rc int
   declare @svr nvarchar(100)
   declare @subj nvarchar(300)
   declare @msg nvarchar(1000)
   declare @hostname nvarchar(100)
   declare @spid_in int

   select top 1 @spid_in = t.spid, @hostname = 'Host:' + t.hostname + ' blocking.'
   from #tblBlockingSpids t
     join Maintenance.dbo.tblBlockingSpids m on t.spid = m.spid 
   where m.timelogged between dateadd(mi, -10, getdate()) and dateadd(Ss, -15, getdate())

   select @svr = isnull(@@servername, 'NULL')

   --build message string
   create table #temp (eventtype varchar(100), parameters varchar(100), eventinfo varchar(max))

   insert into #temp (eventtype, parameters, eventinfo)
   exec maintenance.dbo.getprocesscmd @spid = @spid_in

   select @msg = rtrim(@hostname) + ' info: ' + isnull(eventinfo, '') 
   from #temp


  declare @subject nvarchar(100)
  set @subject = @@SERVERNAME

  select @subject = 'Block on Host: ' + @subject
 

  exec  msdb..sp_send_dbmail  
	@profile_name =  'SQLAlerts'
	, @recipients  = 'SQLAlerts@alder.com' 
	, @body = @msg
	, @subject = @subject
	, @importance ='HIGH'  

   truncate table Maintenance.dbo.tblBlockingSpids

   drop table #temp

end


insert into maintenance.dbo.tblBlockingSpids (spid, cmd, hostname, program_name, loginame, timelogged)
select spid, cmd, hostname, program_name, loginame, timelogged
from #tblBlockingSpids
  

drop table #blocks
drop table #tblBlockingSpids
GO
/****** Object:  StoredProcedure [dbo].[spGetWinEventLogs]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[spGetWinEventLogs] (
	@DateBeg datetime = NULL )
as


if @DateBeg IS NULL or @DateBeg = '1/1/1900'
begin
  set @DateBeg = cast(convert(varchar(100), getdate(), 101) as datetime)
end

declare @DateEnd datetime
 
Set @dateEnd = dateadd(d, 1, @DateBeg)

--SELECT @DateBeg, @Dateend

select * from WinEventLogs
where GenDateTime between @DateBeg and @DateEnd
  and Message not like 'Login failed for user%'
  and SourceName not in ('Print', 'TermServDevices', 'Microsoft-Windows-TerminalServices-Printers')
  and LogFile not in ('SmartConnect', 'eConnect')
order by ComputerName, GenDateTime desc
GO
/****** Object:  StoredProcedure [dbo].[spPerfMonResults]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[spPerfMonResults]

	@dCounterDate datetime = NULL

as 
 
if @dCounterDate is null
begin
	select @dCounterDate = max(CONVERT(varchar, fTime, 101)) from mnttblPerfMonResultsAll 
end

select fServerName, CONVERT(varchar, fTime, 101) as Date, sum(fMemoryPagesSec) / count(*) as AvgPaging
	, sum(fProcessorTime) / count(*) As AvgCPU, sum(fBufferCache) / count(*) as AvgBufferCache
	, sum(fTransactionsSec) / count(*) As AvgTransSec, sum(fUserConnections) / count(*) As AvgUserConns
from mnttblPerfMonResultsAll
where datepart(hour, fTime) between '01' and '23'
  and CONVERT(varchar, fTime, 101) = @dCounterDate
group by fServerName, CONVERT(varchar, fTime, 101)
GO
/****** Object:  StoredProcedure [dbo].[spPerfMonResultsDetail]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
create proc [dbo].[spPerfMonResultsDetail]

	@dCounterDate datetime = NULL,
	@fServerName varchar(50)

as 
 
if @dCounterDate is null
begin
	select @dCounterDate = max(CONVERT(varchar, fTime, 101)) from mnttblPerfMonResultsAll 
end

select *
from mnttblPerfMonResultsAll
where CONVERT(varchar, fTime, 101) = @dCounterDate
  and fServerName = @fServerName
order by fTime
GO
/****** Object:  StoredProcedure [dbo].[spSQLErrorLogs]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE proc [dbo].[spSQLErrorLogs] (
  @DateBeg datetime = NULL
)
as

set transaction isolation level read uncommitted

if @DateBeg < '1/1/1901'
begin
  set @DateBeg = getdate() -1
end

if @DateBeg IS NULL
begin
  set @DateBeg = cast(convert(varchar(100), getdate(), 101) as datetime)
end
  
declare @DateEnd datetime
 
Set @dateEnd = dateadd(d, 1, @DateBeg)

--select @DateBeg, @DateEnd

select * from SQLErrorLogs
where LogEntryDate between @DateBeg and @DateEnd
  and LogEntry not like 'Setting database option SINGLE_USER to ON for database%'
  and LogEntry not like 'Starting up database%'
  and LogEntry not like '%is marked RESTORING and is in a state that does not allow recovery to be run%'
  and LogEntry not like 'Setting database option MULTI_USER to ON for database%'
  and LogEntry not like 'Log was restored. Database:%'
  and LogEntry not like 'Database backed up. Database:%'
  and LogEntry not like 'BACKUP LOG WITH TRUNCATE_ONLY or WITH NO_LOG is deprecated.%'
  and LogEntry not like 'Database was restored: Database:%'
  and LogEntry not like '%This is an informational message only; no user action is required.%'
  and LogEntry not like '%This is an informational message only. No user action is required.%'
  and LogEntry not like 'Login failed for user%'
  and LogEntry not like '%found 0 errors and repaired 0 errors.%'
  and LogEntry not like 'Error: 18456, Severity: 14, State%'
  and LogEntry not like '% cachestore (part of plan cache) due to some database maintenance or reconfigure operations.'
order by Server, LogEntryDate desc
GO
/****** Object:  StoredProcedure [dbo].[SQLAlertSendmail]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROC [dbo].[SQLAlertSendmail]  
  
  @subject nvarchar(100),  
  @message nvarchar(250)  
  
AS  
  
set nocount on  
  
 exec  msdb..sp_send_dbmail  
 @profile_name = 'SQLAlerts'  
 ,@recipients = 'chrisb@sqldatabasesolutions.com'  
 ,@body = @message  
 ,@subject = @subject  
 ,@importance ='HIGH'  
  
select 1
GO
/****** Object:  StoredProcedure [dbo].[sys_sp_SearchCode]    Script Date: 7/1/2019 10:17:26 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[sys_sp_SearchCode]
(
@sText 	varchar(100),
@RowsReturned	int = NULL	OUT
)
AS
-- =============================================
-- AUTHOR: Chris Becker             
-- Search the stored proceudre, UDF, trigger code for a given keyword.
-- Last Updated 6/13/2014
-- **** Please edit this from the file in the repo only.  Changes made directly will be over written ****
-- =============================================
BEGIN
	SET NOCOUNT ON

	SELECT	DISTINCT USER_NAME(o.uid) + '.' + OBJECT_NAME(c.id) AS 'Object name',
		CASE 
 			WHEN OBJECTPROPERTY(c.id, 'IsReplProc') = 1 
				THEN 'Replication stored procedure'
 			WHEN OBJECTPROPERTY(c.id, 'IsExtendedProc') = 1 
				THEN 'Extended stored procedure'				
			WHEN OBJECTPROPERTY(c.id, 'IsProcedure') = 1 
				THEN 'Stored Procedure' 
			WHEN OBJECTPROPERTY(c.id, 'IsTrigger') = 1 
				THEN 'Trigger' 
			WHEN OBJECTPROPERTY(c.id, 'IsTableFunction') = 1 
				THEN 'Table-valued function' 
			WHEN OBJECTPROPERTY(c.id, 'IsScalarFunction') = 1 
				THEN 'Scalar-valued function'
 			WHEN OBJECTPROPERTY(c.id, 'IsInlineFunction') = 1 
				THEN 'Inline function'	
		END AS 'Object type',
		'EXEC sp_helptext ''' + USER_NAME(o.uid) + '.' + OBJECT_NAME(c.id) + '''' AS 'Run this command to see the object text'
	FROM	syscomments c
		INNER JOIN
		sysobjects o
		ON c.id = o.id
	WHERE	c.text LIKE '%' + @sText + '%'	AND
		encrypted = 0				AND
		(
		OBJECTPROPERTY(c.id, 'IsReplProc') = 1		OR
		OBJECTPROPERTY(c.id, 'IsExtendedProc') = 1	OR
		OBJECTPROPERTY(c.id, 'IsProcedure') = 1		OR
		OBJECTPROPERTY(c.id, 'IsTrigger') = 1		OR
		OBJECTPROPERTY(c.id, 'IsTableFunction') = 1	OR
		OBJECTPROPERTY(c.id, 'IsScalarFunction') = 1	OR
		OBJECTPROPERTY(c.id, 'IsInlineFunction') = 1	
		)

	ORDER BY	'Object type', 'Object name'

	SET @RowsReturned = @@ROWCOUNT
END




GO
