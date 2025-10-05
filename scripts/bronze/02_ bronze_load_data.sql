/*
===============================================================================
DDL Script: Load Data into Bronze Layer
===============================================================================
Script Purpose:
    This stored procedure loads raw CSV files from local directories into 
    the Bronze Layer of the data warehouse. 
    The Bronze Layer serves as the raw ingestion zone, containing 
    unprocessed data directly from the source systems.

Details:
    - Handles data ingestion for both CRM and ERP source files.
    - Performs a full refresh by truncating target tables before loading.
    - Uses BULK INSERT for efficient file-to-table loading.
    - Tracks start and end times for each load operation.
    - Implements structured error handling for troubleshooting.

Data Sources:
    - CRM Files: cust_info.csv, prd_info.csv, sales_details.csv
    - ERP Files: LOC_A101.csv, CUST_AZ12.csv, PX_CAT_G1V2.csv

File Path Structure:
    C:\Users\amora\Documents\Projects\dwh_project\datasets\
        ├── source_crm\
        │     ├── cust_info.csv
        │     ├── prd_info.csv
        │     └── sales_details.csv
        └── source_erp\
              ├── LOC_A101.csv
              ├── CUST_AZ12.csv
              └── PX_CAT_G1V2.csv

Usage:
    EXEC bronze.load_bronze;

Notes:
    - Ensure SQL Server has read access to the specified file paths.
    - The procedure should be executed prior to loading the Silver Layer.
===============================================================================
*/



CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME,
        @base_path NVARCHAR(500),
        @crm_path NVARCHAR(500),
        @erp_path NVARCHAR(500),
        @sql NVARCHAR(MAX);

    BEGIN TRY
        --==================================================
        -- 1. Set Base Paths
        --==================================================
        SET @base_path = 'C:\Users\amora\Documents\Projects\dwh_project\datasets\';
        SET @crm_path = @base_path + 'source_crm\';
        SET @erp_path = @base_path + 'source_erp\';

        SET @batch_start_time = GETDATE();

        PRINT '================================================';
        PRINT 'Starting Bronze Layer Load';
        PRINT '================================================';

        --==================================================
        -- CRM TABLES
        --==================================================
        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------';

        ----------------------------------------------------
        -- CRM_CUST_INFO
        ----------------------------------------------------
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        SET @sql = '
            BULK INSERT bronze.crm_cust_info
            FROM ''' + @crm_path + 'cust_info.csv''
            WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);
        ';
        EXEC (@sql);

        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        ----------------------------------------------------
        -- CRM_PRD_INFO
        ----------------------------------------------------
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_prd_info;

        SET @sql = '
            BULK INSERT bronze.crm_prd_info
            FROM ''' + @crm_path + 'prd_info.csv''
            WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);
        ';
        EXEC (@sql);

        SET @end_time = GETDATE();
        PRINT '>> Loaded bronze.crm_prd_info in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        ----------------------------------------------------
        -- CRM_SALES_DETAILS
        ----------------------------------------------------
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.crm_sales_details;

        SET @sql = '
            BULK INSERT bronze.crm_sales_details
            FROM ''' + @crm_path + 'sales_details.csv''
            WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);
        ';
        EXEC (@sql);

        SET @end_time = GETDATE();
        PRINT '>> Loaded bronze.crm_sales_details in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        --==================================================
        -- ERP TABLES
        --==================================================
        PRINT '------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------';

        ----------------------------------------------------
        -- ERP_LOC_A101
        ----------------------------------------------------
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_loc_a101;

        SET @sql = '
            BULK INSERT bronze.erp_loc_a101
            FROM ''' + @erp_path + 'LOC_A101.csv''
            WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);
        ';
        EXEC (@sql);

        SET @end_time = GETDATE();
        PRINT '>> Loaded bronze.erp_loc_a101 in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        ----------------------------------------------------
        -- ERP_CUST_AZ12
        ----------------------------------------------------
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_cust_az12;

        SET @sql = '
            BULK INSERT bronze.erp_cust_az12
            FROM ''' + @erp_path + 'CUST_AZ12.csv''
            WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);
        ';
        EXEC (@sql);

        SET @end_time = GETDATE();
        PRINT '>> Loaded bronze.erp_cust_az12 in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        ----------------------------------------------------
        -- ERP_PX_CAT_G1V2
        ----------------------------------------------------
        SET @start_time = GETDATE();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        SET @sql = '
            BULK INSERT bronze.erp_px_cat_g1v2
            FROM ''' + @erp_path + 'PX_CAT_G1V2.csv''
            WITH (FIRSTROW = 2, FIELDTERMINATOR = '','', TABLOCK);
        ';
        EXEC (@sql);

        SET @end_time = GETDATE();
        PRINT '>> Loaded bronze.erp_px_cat_g1v2 in ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        --==================================================
        -- COMPLETION
        --==================================================
        SET @batch_end_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Bronze Layer Load Completed Successfully';
        PRINT '   - Total Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
        PRINT '==========================================';
    END TRY

    BEGIN CATCH
        PRINT '==========================================';
        PRINT 'ERROR OCCURRED DURING BRONZE LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '==========================================';
    END CATCH
END;
GO

-- Execute procedure
EXEC bronze.load_bronze;
GO

