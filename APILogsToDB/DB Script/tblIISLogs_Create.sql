CREATE TABLE tblIISLogsOriginal
(
    LogID INT IDENTITY(1,1) PRIMARY KEY,          -- Auto-incrementing unique identifier
    [Date] DATE NOT NULL,                        -- Date field from the log (index 0)
    [Time] TIME(0) NOT NULL,                     -- Time field from the log (index 1)
    SIp VARCHAR(15) NOT NULL,                    -- Server IP address (index 2)
    CsMethod VARCHAR(10) NOT NULL,               -- HTTP method (index 3)
    CsUriStem NVARCHAR(255) NOT NULL,            -- URI stem (index 4)
    CsUriQuery NVARCHAR(MAX) NULL,               -- URI query (index 5)
    SPort INT NOT NULL,                          -- Server port (index 6)
    CsUsername NVARCHAR(255) NULL,               -- Username (index 7)
    CIp VARCHAR(15) NOT NULL,                    -- Client IP address (index 8)
    CsUserAgent NVARCHAR(500) NULL,              -- User agent (index 9)
    CsReferer NVARCHAR(1000) NULL,               -- Referer URL (index 10)
    ScStatus INT NOT NULL,                       -- HTTP status code (index 11)
    ScSubstatus INT NOT NULL,                    -- HTTP sub-status code (index 12)
    ScWin32Status INT NOT NULL,                  -- Windows status code (index 13)
    TimeTaken INT NOT NULL                       -- Time taken to process request in ms (index 14)
);
