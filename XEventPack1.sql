
USE MASTER
GO
/*	 ___________________________________________________
	|													|
	|				Extended Event Pack 1				|
	|													|				
	|___________________________________________________|
*/	
BEGIN

BEGIN /*CONFIG*/
DECLARE @FailedQueries INT = 2								--Tracks what Queries are failing	0 for no, 1 for yes, 2 for create with auto start
DECLARE @UserQueries INT = 2								--Tracks what Queries are being run by users	0 for no, 1 for yes, 2 for create with auto start
DECLARE @TempCreation INT = 2								--Gets current Temp Table information	0 for no, 1 for yes, 2 for create with auto start
DECLARE @TempGrowth INT = 2									--Tracks TempDB Growth	0 for no, 1 for yes, 2 for create with auto start
DECLARE @chkdirectory AS NVARCHAR(4000)						--Sets the Directory to store the log files
DECLARE @folder_exists AS INT								--Flag for folder check
DECLARE @Version VARCHAR(4) = SUBSTRING(@@VERSION, 22, 4)	--SQL Server Version
DECLARE @SQLText NVARCHAR(MAX)								--SQL Text to Execute
SET @chkdirectory = N'C:\XEventSessions'					--Change this to store in a different location		

END /*CONFIG*/

BEGIN /*Directory Check*/
DECLARE @file_results TABLE (
	file_exists INT
	,file_is_a_directory INT
	,parent_directory_exists INT
	)

INSERT INTO @file_results (
	file_exists
	,file_is_a_directory
	,parent_directory_exists
	)
EXEC master.dbo.xp_fileexist @chkdirectory

SELECT @folder_exists = file_is_a_directory
FROM @file_results

IF @folder_exists = 0
BEGIN
	PRINT 'Directory does not exists, creating '

	EXECUTE master.dbo.xp_create_subdir @chkdirectory

	PRINT @chkdirectory + 'created on ' + @@servername
END
ELSE
	PRINT 'Directory already exists'
END /*Directory Check*/

BEGIN  /*Extended Event Creation*/

BEGIN  /*Failed Queries*/
IF (@FailedQueries > 0)
	IF NOT EXISTS (
			SELECT NAME
			FROM sys.server_event_sessions
			WHERE NAME = 'FailedQueries'
			)
	BEGIN
		CREATE EVENT SESSION [FailedQueries] ON SERVER 
			ADD EVENT sqlserver.error_reported (ACTION(sqlserver.client_hostname, sqlserver.database_name, sqlserver.sql_text, sqlserver.username) WHERE ([severity] > (10))) ADD TARGET package0.event_file (
			SET filename = @chkdirectory + N'\FailedQueries.xel'
			,max_file_size = (5)
			,max_rollover_files = (5)
			,metadatafile = @chkdirectory + N'\FailedQueries.xem'
			)
			WITH (
					MAX_MEMORY = 4096 KB
					,EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS
					,MAX_DISPATCH_LATENCY = 5 SECONDS
					,MAX_EVENT_SIZE = 0 KB
					,MEMORY_PARTITION_MODE = NONE
					,TRACK_CAUSALITY = OFF
					,STARTUP_STATE = ON
					)

		PRINT '[FailedQueries] created on ' + @@servername

		IF (@FailedQueries = 2)
		BEGIN
			ALTER EVENT SESSION [FailedQueries] ON SERVER STATE = START
		END
	END
END  /*Failed Queries*/

BEGIN  /*User Queries*/
IF (@UserQueries > 0)
	IF NOT EXISTS (
			SELECT NAME
			FROM sys.server_event_sessions
			WHERE NAME = 'UserRunQueries'
			)
	BEGIN
		CREATE EVENT SESSION [UserRunQueries] ON SERVER ADD EVENT sqlserver.sql_batch_completed (SET collect_batch_text = (1) ACTION(sqlos.worker_address, sqlserver.client_app_name, sqlserver.client_hostname, sqlserver.database_name, sqlserver.nt_username, sqlserver.session_id, sqlserver.session_nt_username, sqlserver.sql_text, sqlserver.tsql_stack, sqlserver.username) 
    WHERE ([sqlserver].[like_i_sql_unicode_string]([sqlserver].[client_app_name],N'%Query%')))
		 
		ADD TARGET package0.event_file (
			SET filename = @chkdirectory + N'\User Run Queries.xel'
			,max_file_size = (5)
			)
			WITH (
					MAX_MEMORY = 4096 KB
					,EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS
					,MAX_DISPATCH_LATENCY = 3 SECONDS
					,MAX_EVENT_SIZE = 0 KB
					,MEMORY_PARTITION_MODE = NONE
					,TRACK_CAUSALITY = OFF
					,STARTUP_STATE = ON
					)

		PRINT '[UserRunQueries] created on ' + @@servername

		IF (@UserQueries = 2)
		BEGIN
			ALTER EVENT SESSION [UserRunQueries] ON SERVER STATE = START
		END
	END
END  /*User Queries*/

BEGIN  /*Temp Table Creation*/
IF (@TempCreation > 0)
	IF NOT (@Version = '2008') --Will only work on 2012 and later 
	BEGIN
		IF NOT EXISTS (
				SELECT NAME
				FROM sys.server_event_sessions
				WHERE NAME = 'TempTableCreation'
				)
		BEGIN
			CREATE EVENT SESSION [TempTableCreation] ON SERVER ADD EVENT sqlserver.object_created (
				ACTION(sqlserver.client_app_name, sqlserver.client_hostname, sqlserver.server_principal_name, sqlserver.session_id, sqlserver.session_nt_username, sqlserver.sql_text) WHERE (
					[sqlserver].[like_i_sql_unicode_string]([object_name], N'#%')
					AND [ddl_phase] = (1)
					)
				) ADD TARGET package0.event_file (
				SET filename = @chkdirectory + N'\TempTableCreation.xel'
				,max_file_size = (32768)
				,max_rollover_files = (10)
				)
				WITH (
						MAX_MEMORY = 4096 KB
						,EVENT_RETENTION_MODE = NO_EVENT_LOSS
						,MAX_DISPATCH_LATENCY = 30 SECONDS
						,MAX_EVENT_SIZE = 0 KB
						,MEMORY_PARTITION_MODE = NONE
						,TRACK_CAUSALITY = OFF
						,STARTUP_STATE = OFF
						)

			PRINT '[TempTableCreation] created on ' + @@servername

			IF (@TempCreation = 2)
			BEGIN
				ALTER EVENT SESSION [TempTableCreation] ON SERVER STATE = START
			END
		END
	END
END  /*Temp Table Creation*/

BEGIN  /*Temp Table Growth*/
IF (@TempGrowth > 0)
	IF NOT EXISTS (
			SELECT NAME
			FROM sys.server_event_sessions
			WHERE NAME = 'TempDBGrowth'
			)
	BEGIN
		CREATE EVENT SESSION [TempDBGrowth] ON SERVER ADD EVENT sqlserver.database_file_size_change (
			ACTION(sqlserver.client_hostname, sqlserver.database_id, sqlserver.session_id, sqlserver.sql_text) WHERE (
				[database_id] = (2)
				AND [session_id] > (50)
				)
			)
			,ADD EVENT sqlserver.databases_log_file_used_size_changed (
			ACTION(sqlserver.client_hostname, sqlserver.database_id, sqlserver.session_id, sqlserver.sql_text) WHERE (
				[database_id] = (2)
				AND [session_id] > (50)
				)
			) ADD TARGET package0.event_file (
			SET filename = @chkdirectory +N'\TempDBGrowth.xel'
			,max_file_size = (10)
			,max_rollover_files = (10)
			,metadatafile = @chkdirectory +N'\TempDBGrowth.xem'
			)
			WITH (
					MAX_MEMORY = 4096 KB
					,EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS
					,MAX_DISPATCH_LATENCY = 1 SECONDS
					,MAX_EVENT_SIZE = 0 KB
					,MEMORY_PARTITION_MODE = NONE
					,TRACK_CAUSALITY = ON
					,STARTUP_STATE = ON
					)

		PRINT '[TempDBGrowth] created on ' + @@servername

		IF (@TempGrowth = 2)
		BEGIN
			ALTER EVENT SESSION [TempDBGrowth] ON SERVER STATE = START
		END
	END
END  /*Temp Table Growth*/

END  /*Extended Event Creation*/

BEGIN /*Stored Procedure Creation*/

BEGIN  /*Failed Queries*/

IF(@FailedQueries >0)
set @SQLText = '
-- =============================================
-- Author:		Christopher Green
-- Create date: 2018/06/19
-- Description:	Creates the Stored Procedure to Scrape Errors from the FailedQueries Extended Events
-- =============================================
CREATE PROCEDURE sp_GetServerErrors
@Date		DATETIME2 = null,
@Severity	BIGINT = null,
@User		NVARCHAR(50) = null,
@SQL		NVARCHAR(100) = null,
@Error		NVARCHAR(100) = null
AS
BEGIN
SET NOCOUNT ON
;with events_cte as(
select
DATEADD(mi,
DATEDIFF(mi, GETUTCDATE(), CURRENT_TIMESTAMP),
xevents.event_data.value(''(event/@timestamp)[1]'', ''datetime2'')) AS [Timestamp],
xevents.event_data.value(''(event/data[@name="severity"]/value)[1]'', ''bigint'') AS [Error Severity],
xevents.event_data.value(''(event/data[@name="error_number"]/value)[1]'', ''bigint'') AS [Error Number],
xevents.event_data.value(''(event/data[@name="message"]/value)[1]'', ''nvarchar(512)'') AS [Error Message],
xevents.event_data.value(''(event/action[@name="sql_text"]/value)[1]'', ''nvarchar(max)'') AS [SQL Text],
xevents.event_data.value(''(event/action[@name="username"]/value)[1]'', ''nvarchar(max)'') AS [Username],
xevents.event_data.value(''(event/action[@name="database_id"]/value)[1]'', ''nvarchar(max)'') AS [DatabaseID]
from sys.fn_xe_file_target_read_file
('''+ @chkdirectory +'\FailedQueries*.xel'','''
 + @chkdirectory +'\FailedQueries*.xem'',
null, null)
cross apply (select CAST(event_data as XML) as event_data) as xevents
)
SELECT 
	DB_NAME(DatabaseID) as [Database],
	[Timestamp],
	[Error Severity],
	[Error Number],
	[Error Message],
	[SQL Text],
	[Username],
	(CASE	
		WHEN [Error Severity] < 17 THEN ''Issue Caused by query, review SQL Text''
		WHEN [Error Severity] = 17 THEN ''Disk space or lock limit''
		WHEN [Error Severity] = 18 THEN ''Nonfatal internal software error''
		WHEN [Error Severity] = 19 THEN ''Non-configurable internal limit has been exceeded''
		WHEN [Error Severity] = 20 THEN ''Current statement has encountered a problem and because of this severity level client connection with SQL Server will be disconnected''
		WHEN [Error Severity] = 21 THEN ''A problem has been encountered that affects all processes in the current database''
		WHEN [Error Severity] = 22 THEN ''Database table or index may be corrupt or damaged''
		WHEN [Error Severity] = 23 THEN ''Problem with database integrity''
		WHEN [Error Severity] = 24 THEN ''Problem with the hardware of SQL Server''
	END)as [Possible Cause]
from 
	events_cte
	WHERE 1=1
	AND(@Date IS NULL OR CONVERT(DATE,@DATE) LIKE CONVERT(DATE,[Timestamp]))
	AND(@Severity IS NULL OR [Error Severity] >= @Severity)
	AND(@User IS NULL OR [Username] LIKE ''%''+@User+''%'')
	AND(@SQL IS NULL OR [SQL Text] LIKE ''%''+@SQL+''%'')
	AND(@Error IS NULL OR [Error Message] LIKE ''%''+@Error+''%'')
order by 
	[Timestamp];
END'

EXECUTE SP_executeSQL @SqlText
END  /*Failed Queries*/

if(@UserQueries>0)
BEGIN

set @SQLText = '
-- =============================================
-- Author:		Christopher Green
-- Create date: 2018/06/19
-- Description:	Creates the Stored Procedure to Scrape Queries from the UserRunQueries Extended Events
-- =============================================
CREATE PROCEDURE sp_GetUserQueries
@Date		DATETIME2 = null,
@User		NVARCHAR(50) = null,
@SQL		NVARCHAR(100) = null
AS
BEGIN
SET NOCOUNT ON
;with events_cte as(
select
DATEADD(mi,
DATEDIFF(mi, GETUTCDATE(), CURRENT_TIMESTAMP),
xevents.event_data.value(''(event/@timestamp)[1]'', ''datetime2'')) AS [Event Timestamp],
DB_NAME(xevents.event_data.value(''(event/action[@name="database_id"]/value)[1]'', ''nvarchar(max)'')) AS [Database],
xevents.event_data.value(''(event/action[@name="client_hostname"]/value)[1]'', ''nvarchar(max)'') AS [Source],
xevents.event_data.value(''(event/action[@name="username"]/value)[1]'', ''nvarchar(max)'') AS [Username],
xevents.event_data.value(''(event/action[@name="sql_text"]/value)[1]'', ''nvarchar(max)'') AS [SQL Text]
from sys.fn_xe_file_target_read_file
('''+ @chkdirectory +'\User Run Queries*.xel'',
'''+ @chkdirectory +'\User Run Queries*.xem'',
null, null)
cross apply (select CAST(event_data as XML) as event_data) as xevents
)
SELECT *
from events_cte
	WHERE 1=1
	AND(@Date IS NULL OR CONVERT(DATE,@DATE) LIKE CONVERT(DATE,[Event Timestamp])
	AND(@User IS NULL OR [Username] LIKE ''%''+@User+''%'')
	AND(@SQL IS NULL OR [SQL Text] LIKE ''%''+@SQL+''%''))
END
'

EXECUTE SP_executeSQL @SqlText
END /*User Queries*/

if(@TempGrowth>0)
BEGIN /*TempDBGrowth*/
set @SQLText = '
-- =============================================
-- Author:		Christopher Green
-- Create date: 2018/06/19
-- Description:	Creates the Stored Procedure to Scrape Queries from the TempDBGowth Extended Events
-- =============================================
CREATE PROCEDURE sp_GetTempDBGrowth

AS
SET NOCOUNT ON

SELECT [evts].[event_data].[value](''(event/action[@name="session_id"]/value)[1]'',''INT'') AS [SessionID] ,
  [evts].[event_data].[value](''(event/action[@name="client_hostname"]/value)[1]'',''VARCHAR(MAX)'') AS [ClientHostName] ,
  DB_NAME([evts].[event_data].[value](''(event/action[@name="database_id"]/value)[1]'', ''BIGINT'')) AS [OriginatingDB] ,
  DB_NAME([evts].[event_data].[value](''(event/data[@name="database_id"]/value)[1]'', ''BIGINT'')) AS [GrowthDB] ,
  [evts].[event_data].[value](''(event/data[@name="file_name"]/value)[1]'',''VARCHAR(MAX)'') AS [GrowthFile] ,
  [evts].[event_data].[value](''(event/data[@name="file_type"]/text)[1]'', ''VARCHAR(MAX)'') AS [DBFileType] ,
  [evts].[event_data].[value](''(event/@name)[1]'', ''VARCHAR(MAX)'') AS [EventName] ,
  [evts].[event_data].[value](''(event/data[@name="size_change_kb"]/value)[1]'',  ''BIGINT'') AS [SizeChangeInKb] ,
  [evts].[event_data].[value](''(event/data[@name="total_size_kb"]/value)[1]'', ''BIGINT'') AS [TotalFileSizeInKb] ,
  [evts].[event_data].[value](''(event/data[@name="duration"]/value)[1]'', ''BIGINT'') AS [DurationInMS] ,
  [evts].[event_data].[value](''(event/@timestamp)[1]'', ''VARCHAR(MAX)'') AS [GrowthTime] ,
  [evts].[event_data].[value](''(event/action[@name="sql_text"]/value)[1]'', ''VARCHAR(MAX)'') AS [QueryText]
FROM ( SELECT CAST([event_data] AS XML) AS [TargetData]
  FROM [sys].[fn_xe_file_target_read_file]('''+ @chkdirectory +'\TempDBGrowth*.xel'', NULL, NULL, NULL) ) AS [evts] ( [event_data] )
WHERE   [evts].[event_data].[value](''(event/@name)[1]'', ''VARCHAR(MAX)'') = ''database_file_size_change''
  OR [evts].[event_data].[value](''(event/@name)[1]'', ''VARCHAR(MAX)'') = ''databases_log_file_used_size_changed''
ORDER BY [GrowthTime] ASC;'

EXECUTE SP_executeSQL @SqlText
END/*TempDBGrowth*/
if(@TempCreation>0)
IF NOT (@Version = '2008')
BEGIN /*TempDBCreation*/


set @SQLText = '
-- =============================================
-- Author:		Christopher Green
-- Create date: 2018/06/19
-- Description:	Creates the Stored Procedure to Scrape Queries from the TempDBCreation Extended Events
-- =============================================
CREATE PROCEDURE sp_GetTempDBCreation
AS
DECLARE @delta INT = DATEDIFF(MINUTE, SYSUTCDATETIME(), SYSDATETIME());
 SET NOCOUNT ON
;WITH xe AS
(
  SELECT 
    [obj_name]  = xe.d.value(N''(event/data[@name="object_name"]/value)[1]'',N''sysname''),
    [object_id] = xe.d.value(N''(event/data[@name="object_id"]/value)[1]'',N''int''),
    [timestamp] = DATEADD(MINUTE, @delta, xe.d.value(N''(event/@timestamp)[1]'',N''datetime2'')),
    SPID        = xe.d.value(N''(event/action[@name="session_id"]/value)[1]'',N''int''),
    NTUserName  = xe.d.value(N''(event/action[@name="session_nt_username"]/value)[1]'',N''sysname''),
    SQLLogin    = xe.d.value(N''(event/action[@name="server_principal_name"]/value)[1]'',N''sysname''),
    HostName    = xe.d.value(N''(event/action[@name="client_hostname"]/value)[1]'',N''sysname''),
    AppName     = xe.d.value(N''(event/action[@name="client_app_name"]/value)[1]'',N''nvarchar(max)''),
    SQLBatch    = xe.d.value(N''(event/action[@name="sql_text"]/value)[1]'',N''nvarchar(max)'')
 FROM 
    sys.fn_xe_file_target_read_file(N'''+ @chkdirectory +'\TempTableCreation*.xel'',NULL,NULL,NULL) AS ft
    CROSS APPLY (SELECT CONVERT(XML, ft.event_data)) AS xe(d)
) 
SELECT 
  DefinedName         = xe.obj_name,
  GeneratedName       = o.name,
  o.[object_id],
  xe.[timestamp],
  o.create_date,
  xe.SPID,
  xe.NTUserName,
  xe.SQLLogin, 
  xe.HostName,
  ApplicationName     = xe.AppName,
  TextData            = xe.SQLBatch,
  row_count           = x.rc,
  reserved_page_count = x.rpc
FROM xe
INNER JOIN tempdb.sys.objects AS o
ON o.[object_id] = xe.[object_id]
AND o.create_date >= DATEADD(SECOND, -2, xe.[timestamp])
AND o.create_date <= DATEADD(SECOND,  2, xe.[timestamp])
INNER JOIN
(
  SELECT 
    [object_id],
    rc  = SUM(CASE WHEN index_id IN (0,1) THEN row_count END), 
    rpc = SUM(reserved_page_count)
  FROM tempdb.sys.dm_db_partition_stats
  GROUP BY [object_id]
) AS x
ON o.[object_id] = x.[object_id];'
EXECUTE SP_executeSQL @SqlText
END/*TempDBCreation*/

END /*Stored Procedure Creation*/


END