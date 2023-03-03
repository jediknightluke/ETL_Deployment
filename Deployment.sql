---------------------------------------------------------------------------------------
--                  Implementation of self contained ETL using SQL scripts           --
--                                                                                   --
--                                  By Luke Melton                                   --
--                                    02/25/2023                                     --
---------------------------------------------------------------------------------------

DECLARE @CURRENT_TIME DATETIME = CURRENT_TIMESTAMP
DECLARE @USER VARCHAR(200) = SELECT USER_NAME();

PRINT 'Beginning Deployment of ETL'
PRINT 'Steps are as follows:'
PRINT 'Step 1: Create Tables for logging, source information, and imports'
PRINT 'Step 2: Create Stored Procedures for handling the ETL logic and seeding the data'
PRINT 'Step 3: Create Views for a cleaned up version of the data'



CREATE TABLE [dbo].[etl_log] (
    [ETL_ID] [int] NOT NULL,
    [Task]   [varchar](100) NOT NULL,
    [Message] [varchar](500) NOT NULL,
    [Date] datetime default CURRENT_TIMESTAMP,
    [User] [varchar](200) NOT NULL
)


CREATE TABLE [dbo].[etl_status] (
    [ETL_ID] [int] NOT NULL,
    [StartTime] datetime NOT NULL,
    [UserID] [varchar](200) NOT NULL,
    [ETL_Status] varchar(8) NULL,
    [EndTime] datetime NOT NULL
)

CREATE TABLE [dbo].[etl_components] (
    [ParameterName] [varchar](200) NOT NULL,
    [ParameterValue] [varchar](MAX) NOT NULL,
    [ParameterDescription] varchar(MAX) NULL
)

CREATE TABLE [dbo].[SourcesImport] (
    [SourceID] [int] NOT NULL,
    [Schema] [varchar](20) NOT NULL,
    [TableName] [varchar](200) NOT NULL,
    [SourceFile] [varchar](200) NOT NULL,
    [FileType] [char](10) NOT NULL,
    [Delimiter] [char](5) NULL,
    [FirstRow] [int] NOT NULL,
    [Terminator] [char](10) NOT NULL,
    [Active] [char](1) NOT NULL
)

CREATE TABLE [dbo].[IMPORTSOURCE_1] (

)

CREATE TABLE [dbo].[IMPORTSOURCE_2] (
    
)

CREATE TABLE [dbo].[IMPORTSOURCE_3] (
    
)

CREATE TABLE [dbo].[IMPORTSOURCE_4] (
    
)

CREATE TABLE [dbo].[IMPORTSOURCE_5] (
    
)


CREATE PROCEDURE [dbo].[sp_etl_logging]
    @Message VARCHAR(MAX),
    @Task VARCHAR(200)
AS

    DECLARE @ETL_ID INT
    SELECT @ETL_ID = COALESCE(MAX(ETL_ID), -1) FROM [dbo].[etl_log]

    INSERT INTO [dbo].[etl_log] (ETL_ID,Task,Message)
    VALUES (@ETL_ID, @Task, @Message)


CREATE PROCEDURE [dbo].[sp_etl_ImportSources] 
    @ProcedureName varchar(200) = "sp_etl_logging"
AS

    BEGIN TRY

        DECLARE db_cursor CURSOR FOR 
        SELECT [Schema], [TableName], [SourceFile], [Delimiter], [FirstRow], [Terminator] 
        FROM dbo.SourcesImport
        WHERE [Active] = 'T'

        OPEN db_cursor  
        FETCH NEXT FROM db_cursor INTO @name  

        WHILE @@FETCH_STATUS = 0  
        BEGIN  
            BULK INSERT 

            FETCH NEXT FROM db_cursor INTO @name 
        END 

        CLOSE db_cursor  
        DEALLOCATE db_cursor 


    END TRY

    BEGIN CATCH
        DECLARE @ErrorMessage VARCHAR(MAX)
        SET @ErrorMessage = @ProcedureName + "-" + ERROR_MESSAGE()

        RAISERROR (@ErrorMessage);
    END CATCH



CREATE PROCEDURE [dbo].[sp_etl_SeedImportData] 
    @ProcedureName varchar(200) = "sp_etl_logging"
AS

    BEGIN TRY

    INSERT INTO [dbo].[SourcesImport] VALUES (1, 'dbo', 'IMPORTSOURCE_1', 'SOURCEFILE_1', 'Flat', '|', '2', 'CRLF')
    INSERT INTO [dbo].[SourcesImport] VALUES (1, 'dbo', 'IMPORTSOURCE_1', 'SOURCEFILE_1', 'Flat', '|', '2', 'CRLF')
    INSERT INTO [dbo].[SourcesImport] VALUES (1, 'dbo', 'IMPORTSOURCE_1', 'SOURCEFILE_1', 'Flat', '|', '2', 'CRLF')
    INSERT INTO [dbo].[SourcesImport] VALUES (1, 'dbo', 'IMPORTSOURCE_1', 'SOURCEFILE_1', 'Flat', '|', '2', 'CRLF')
    INSERT INTO [dbo].[SourcesImport] VALUES (1, 'dbo', 'IMPORTSOURCE_1', 'SOURCEFILE_1', 'Flat', '|', '2', 'CRLF')
    INSERT INTO [dbo].[SourcesImport] VALUES (1, 'dbo', 'IMPORTSOURCE_1', 'SOURCEFILE_1', 'Flat', '|', '2', 'CRLF')
    INSERT INTO [dbo].[SourcesImport] VALUES (1, 'dbo', 'IMPORTSOURCE_1', 'SOURCEFILE_1', 'Flat', '|', '2', 'CRLF')
    INSERT INTO [dbo].[SourcesImport] VALUES (1, 'dbo', 'IMPORTSOURCE_1', 'SOURCEFILE_1', 'Flat', '|', '2', 'CRLF')
    INSERT INTO [dbo].[SourcesImport] VALUES (1, 'dbo', 'IMPORTSOURCE_1', 'SOURCEFILE_1', 'Flat', '|', '2', 'CRLF')
    INSERT INTO [dbo].[SourcesImport] VALUES (1, 'dbo', 'IMPORTSOURCE_1', 'SOURCEFILE_1', 'Flat', '|', '2', 'CRLF')
    INSERT INTO [dbo].[SourcesImport] VALUES (1, 'dbo', 'IMPORTSOURCE_1', 'SOURCEFILE_1', 'Flat', '|', '2', 'CRLF')



    INSERT INTO [dbo].[SourcesImport] VALUES (1, 'dbo', 'IMPORTSOURCE_1', 'SOURCEFILE_1', 'Flat', '|', '2', 'CRLF')
    INSERT INTO [dbo].[SourcesImport] VALUES (1, 'dbo', 'IMPORTSOURCE_1', 'SOURCEFILE_1', 'Flat', '|', '2', 'CRLF')
    INSERT INTO [dbo].[SourcesImport] VALUES (1, 'dbo', 'IMPORTSOURCE_1', 'SOURCEFILE_1', 'Flat', '|', '2', 'CRLF')
    INSERT INTO [dbo].[SourcesImport] VALUES (1, 'dbo', 'IMPORTSOURCE_1', 'SOURCEFILE_1', 'Flat', '|', '2', 'CRLF')
    INSERT INTO [dbo].[SourcesImport] VALUES (1, 'dbo', 'IMPORTSOURCE_1', 'SOURCEFILE_1', 'Flat', '|', '2', 'CRLF')
    INSERT INTO [dbo].[SourcesImport] VALUES (1, 'dbo', 'IMPORTSOURCE_1', 'SOURCEFILE_1', 'Flat', '|', '2', 'CRLF')
    INSERT INTO [dbo].[SourcesImport] VALUES (1, 'dbo', 'IMPORTSOURCE_1', 'SOURCEFILE_1', 'Flat', '|', '2', 'CRLF')



    END TRY

    BEGIN CATCH
        DECLARE @ErrorMessage VARCHAR(MAX)
        SET @ErrorMessage = @ProcedureName + "-" + ERROR_MESSAGE()

        RAISERROR (@ErrorMessage);
    END CATCH
