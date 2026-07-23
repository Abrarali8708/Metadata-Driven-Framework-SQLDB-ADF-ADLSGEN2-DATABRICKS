-- ============================================================
-- 1. CONTROL FRAMEWORK SETUP (ctl Schema)
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'ctl')
BEGIN
    EXEC('CREATE SCHEMA ctl;');
END
GO

-- 1.1 Source Systems Metadata Table
IF OBJECT_ID('ctl.SourceSystem', 'U') IS NULL
CREATE TABLE ctl.SourceSystem (
    SourceSystemId      INT IDENTITY(1,1) PRIMARY KEY,
    SourceSystemName    VARCHAR(100) NOT NULL UNIQUE,
    SourceType          VARCHAR(30)  NOT NULL, -- 'AzureSql', 'OnPremSql', 'REST', etc.
    LinkedServiceName   VARCHAR(100) NOT NULL,
    IntegrationRuntime  VARCHAR(60)  NOT NULL,
    IsEnabled           BIT          NOT NULL DEFAULT 1,
    CreatedUtc          DATETIME2    DEFAULT SYSUTCDATETIME()
);
GO

-- 1.2 Entity Configuration Table
IF OBJECT_ID('ctl.EntityConfig', 'U') IS NULL
CREATE TABLE ctl.EntityConfig (
    EntityId            INT IDENTITY(1,1) PRIMARY KEY,
    SourceSystemId      INT          NOT NULL FOREIGN KEY REFERENCES ctl.SourceSystem(SourceSystemId),
    SourceObject        VARCHAR(200) NOT NULL, -- e.g. 'sales.Orders'
    TargetContainer     VARCHAR(100) NOT NULL DEFAULT 'lake',
    TargetPath          VARCHAR(200) NOT NULL, -- e.g. 'raw/sales/orders'
    LoadType            VARCHAR(20)  NOT NULL DEFAULT 'Incremental', -- 'Full' or 'Incremental'
    WatermarkColumn     VARCHAR(100) NULL,     -- e.g. 'UpdatedAt'
    WatermarkType       VARCHAR(20)  NULL,     -- 'DateTime' or 'BigInt'
    PrimaryKeyCols      VARCHAR(200) NULL,     -- e.g. 'OrderId'
    FileFormat          VARCHAR(20)  NOT NULL DEFAULT 'Parquet',
    IsEnabled           BIT          NOT NULL DEFAULT 1
);
GO

-- 1.3 Watermark Tracker Table
IF OBJECT_ID('ctl.Watermark', 'U') IS NULL
CREATE TABLE ctl.Watermark (
    EntityId            INT          PRIMARY KEY FOREIGN KEY REFERENCES ctl.EntityConfig(EntityId),
    WatermarkValue      VARCHAR(100) NOT NULL,
    LastUpdatedUtc      DATETIME2    DEFAULT SYSUTCDATETIME()
);
GO

-- 1.4 Audit Pipeline Run Log
IF OBJECT_ID('ctl.PipelineRunLog', 'U') IS NULL
CREATE TABLE ctl.PipelineRunLog (
    LogId               BIGINT IDENTITY(1,1) PRIMARY KEY,
    RunId               VARCHAR(100) NOT NULL,
    EntityId            INT          NULL,
    PipelineName        VARCHAR(100) NOT NULL,
    Status              VARCHAR(20)  NOT NULL, -- 'InProgress', 'Succeeded', 'Failed'
    RowsRead            INT          NULL,
    RowsWritten         INT          NULL,
    ErrorMessage        VARCHAR(MAX) NULL,
    StartTimeUtc        DATETIME2    NOT NULL,
    EndTimeUtc          DATETIME2    NULL
);
GO

-- ============================================================
-- 2. STORED PROCEDURES FOR ADF ORCHESTRATION
-- ============================================================

-- Procedure 1: Fetch active entities to ingest
CREATE OR ALTER PROCEDURE ctl.usp_GetEntitiesToLoad
AS
BEGIN
    SET NOCOUNT ON;
    SELECT 
        e.EntityId,
        s.SourceSystemName,
        s.SourceType,
        s.LinkedServiceName,
        s.IntegrationRuntime,
        e.SourceObject,
        e.TargetContainer,
        e.TargetPath,
        e.LoadType,
        e.WatermarkColumn,
        e.WatermarkType,
        e.PrimaryKeyCols,
        e.FileFormat,
        ISNULL(w.WatermarkValue, '1900-01-01 00:00:00') AS CurrentWatermark
    FROM ctl.EntityConfig e
    JOIN ctl.SourceSystem s ON e.SourceSystemId = s.SourceSystemId
    LEFT JOIN ctl.Watermark w ON e.EntityId = w.EntityId
    WHERE e.IsEnabled = 1 AND s.IsEnabled = 1;
END
GO

-- Procedure 2: Advance Watermark on Success
CREATE OR ALTER PROCEDURE ctl.usp_UpdateWatermark
    @EntityId INT,
    @NewWatermarkValue VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    MERGE ctl.Watermark AS target
    USING (SELECT @EntityId AS EntityId, @NewWatermarkValue AS WatermarkValue) AS source
    ON (target.EntityId = source.EntityId)
    WHEN MATCHED THEN
        UPDATE SET Target.WatermarkValue = source.WatermarkValue, Target.LastUpdatedUtc = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (EntityId, WatermarkValue, LastUpdatedUtc)
        VALUES (source.EntityId, source.WatermarkValue, SYSUTCDATETIME());
END
GO

-- this procedure takes value from the pipeline and pipeline gives current timestamps so there can be a 
--case that we dont have any records > current timestamp in table so we should consider max timestamp 
--from updatedat column thats why altering store procedure 

CREATE OR ALTER PROCEDURE [ctl].[usp_UpdateWatermark]
    @EntityId INT
AS
BEGIN
    UPDATE w
    SET w.WatermarkValue = MAX_SRC.MaxUpdated
    FROM ctl.Watermark w
    JOIN ctl.EntityConfig e ON w.EntityId = e.EntityId
    CROSS APPLY (
        -- Dynamically fetches the maximum value of the source table's watermark column
        SELECT MAX(CAST(UpdatedAt AS VARCHAR(50))) AS MaxUpdated 
        FROM sales.Orders -- (or use dynamic SQL to read e.SourceObject)
    ) MAX_SRC
    WHERE w.EntityId = @EntityId;
END

-- Procedure 3: Log Pipeline Runs
CREATE OR ALTER PROCEDURE ctl.usp_LogPipelineRun
    @RunId VARCHAR(100),
    @EntityId INT = NULL,
    @PipelineName VARCHAR(100),
    @Status VARCHAR(20),
    @RowsRead INT = NULL,
    @RowsWritten INT = NULL,
    @ErrorMessage VARCHAR(MAX) = NULL,
    @StartTimeUtc DATETIME2 = NULL
AS
BEGIN
    SET NOCOUNT ON;
    IF @Status = 'InProgress'
    BEGIN
        INSERT INTO ctl.PipelineRunLog (RunId, EntityId, PipelineName, Status, StartTimeUtc)
        VALUES (@RunId, @EntityId, @PipelineName, @Status, ISNULL(@StartTimeUtc, SYSUTCDATETIME()));
    END
    ELSE
    BEGIN
        UPDATE ctl.PipelineRunLog
        SET Status = @Status,
            RowsRead = @RowsRead,
            RowsWritten = @RowsWritten,
            ErrorMessage = @ErrorMessage,
            EndTimeUtc = SYSUTCDATETIME()
        WHERE RunId = @RunId;
    END
END
GO

-- ============================================================
-- 3. SAMPLE SOURCE DATA CREATION (sales Schema)
-- ============================================================
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'sales')
BEGIN
    EXEC('CREATE SCHEMA sales;');
END
GO

IF OBJECT_ID('sales.Orders', 'U') IS NULL
CREATE TABLE sales.Orders (
    OrderId     INT IDENTITY(1,1) PRIMARY KEY,
    CustomerId  INT,
    OrderAmount DECIMAL(10,2),
    OrderStatus VARCHAR(20),
    UpdatedAt   DATETIME2 DEFAULT SYSUTCDATETIME()
);
GO

-- Insert initial sample rows
INSERT INTO sales.Orders (CustomerId, OrderAmount, OrderStatus, UpdatedAt) VALUES
(101, 250.00, 'COMPLETED', '2024-01-10 10:00:00'),
(102, 120.50, 'PENDING',   '2024-01-10 11:30:00'),
(103, 499.99, 'COMPLETED', '2024-01-11 09:15:00');
GO

-- Seed Control Metadata
INSERT INTO ctl.SourceSystem (SourceSystemName, SourceType, LinkedServiceName, IntegrationRuntime)
VALUES ('SalesOLTP_DB', 'AzureSql', 'LS_AzureSql_Practicedb', 'AutoResolveIntegrationRuntime');

INSERT INTO ctl.EntityConfig (SourceSystemId, SourceObject, TargetContainer, TargetPath, LoadType, WatermarkColumn, WatermarkType, PrimaryKeyCols)
VALUES (1, 'sales.Orders', 'lake', 'raw/sales/orders', 'Incremental', 'UpdatedAt', 'DateTime', 'OrderId');
GO