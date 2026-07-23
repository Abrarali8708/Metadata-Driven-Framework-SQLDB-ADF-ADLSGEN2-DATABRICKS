-- Ingesting data into DB:
-- 1. Register the Azure SQL DB Source System
INSERT INTO ctl.SourceSystem (
    SourceSystemName, 
    SourceType, 
    LinkedServiceName, 
    IntegrationRuntime,
    IsEnabled,           -- Explicitly declared
    CreatedUtc           -- Explicitly declared
)
VALUES (
    'SalesOLTP_DB', 
    'AzureSql', 
    'LS_AzureSql_Practicedb', 
    'AutoResolveIntegrationRuntime',
    1,                   -- Explicitly passing 1
    SYSUTCDATETIME()     -- Explicitly passing the function
);

-- Get the inserted SourceSystemID
-- 1. Get the inserted SourceSystemId dynamically
DECLARE @SourceSystemID INT = (
    SELECT SourceSystemId 
    FROM ctl.SourceSystem 
    WHERE SourceSystemName = 'SalesOLTP_DB' -- Must match the name you used in the SourceSystem insert
);

-- 2. Register the sales.Orders Entity Configuration
INSERT INTO ctl.EntityConfig (
    SourceSystemId, 
    SourceObject, 
    TargetContainer, 
    TargetPath, 
    LoadType, 
    WatermarkColumn, 
    WatermarkType,
    PrimaryKeyCols,
    FileFormat
)
VALUES (
    @SourceSystemID, 
    'sales.Orders', -- Corrected schema and table name
    'lake', 
    'raw/sales/orders/year=@{formatDateTime(utcNow(),''yyyy'')}/month=@{formatDateTime(utcNow(),''MM'')}', -- Dynamic ADF path
    'Incremental', 
    'UpdatedAt',    -- Corrected watermark column matching our DDL
    'DateTime',
    'OrderId',
    'Parquet'
);
GO

-- 3. Initialize the Watermark Record for Orders
DECLARE @EntityID INT = (
    SELECT EntityId 
    FROM ctl.EntityConfig 
    WHERE SourceObject = 'sales.Orders'
);
-- Insert a baseline watermark (e.g., pulling data only from Jan 1, 2024 onwards)
INSERT INTO ctl.Watermark (
    EntityId, 
    WatermarkValue, 
    LastUpdatedUtc
)
VALUES (
    @EntityID, 
    '2024-01-01 00:00:00', 
    SYSUTCDATETIME()
);
GO

UPDATE ctl.EntityConfig
SET TargetPath = 'raw/sales/orders'
WHERE SourceObject = 'sales.Orders';

select * from [ctl].[SourceSystem];
select * from [ctl].[EntityConfig];
select * from [ctl].[Watermark];
select * from [sales].[Orders];
select * from ctl.PipelineRunLog;

DELETE FROM ctl.PipelineRunLog
WHERE EndTimeUtc IS NULL;
-- Insert initial sample rows
INSERT INTO sales.Orders (CustomerId, OrderAmount, OrderStatus, UpdatedAt) VALUES
(104, 150.00, 'COMPLETED', '2024-07-21 10:00:00'),
(105, 520.50, 'PENDING',   '2024-07-21 11:30:00'),
(106, 409.99, 'COMPLETED', '2024-07-21 09:15:00');
GO


SELECT e.EntityId, s.LinkedServiceName, e.SourceObject,
       e.TargetContainer, e.TargetPath, e.LoadType,
       e.WatermarkColumn, e.WatermarkType,
       ISNULL(w.WatermarkValue, '1900-01-01') AS CurrentWatermark
FROM ctl.EntityConfig e
JOIN ctl.SourceSystem s ON e.SourceSystemId = s.SourceSystemId
LEFT JOIN ctl.Watermark w ON e.EntityId = w.EntityId
WHERE e.IsEnabled = 1 AND s.IsEnabled = 1

INSERT INTO sales.Orders (CustomerId, OrderAmount, OrderStatus, UpdatedAt) 
VALUES
(0987, 6643.00, 'COMPLETED', SYSDATETIME()),
(345643, 843.50, 'PENDING',   SYSDATETIME()),
(342187, 98733.99, 'COMPLETED', SYSDATETIME());
GO

INSERT INTO sales.Orders (CustomerId, OrderAmount, OrderStatus, UpdatedAt) 
VALUES
(898989, 43567.99, 'COMPLETED', SYSDATETIME());
GO

--so suppose all my records are deleted in my dumping zone which is adls gen 2 
--now i want that my pipeline should load full data and after this incremental so what to do
UPDATE w
SET w.WatermarkValue = '1900-01-01',
    w.LastUpdatedUtc = SYSDATETIME()
FROM ctl.Watermark w
WHERE w.EntityId = 1;