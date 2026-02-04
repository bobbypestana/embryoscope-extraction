-- Create metadata table for tracking embryo image extractions
-- This table tracks which embryos have had their images extracted,
-- when they were extracted, and the status of the extraction.

CREATE SCHEMA IF NOT EXISTS gold;

-- Drop existing table if it exists (for development/testing)
DROP TABLE IF EXISTS gold.embryo_images_metadata;

-- Create the metadata table
CREATE TABLE IF NOT EXISTS gold.embryo_images_metadata (
    embryo_id VARCHAR NOT NULL,                        -- Embryo ID (same as Slide ID from data_ploidia)
    focal_plane INTEGER NOT NULL,                      -- Focal plane number (-5 to 5)
    prontuario VARCHAR,                                -- Patient ID (patient_PatientID)
    embryo_description_id VARCHAR,                     -- Embryo description (e.g., AA1, AA12)
    clinic_location VARCHAR NOT NULL,                  -- Clinic name (from patient_unit_huntington)
    extraction_timestamp TIMESTAMP NOT NULL,           -- When the extraction occurred
    image_count INTEGER,                               -- Number of images in the ZIP file
    file_size_bytes BIGINT,                            -- Size of the ZIP file in bytes
    image_runs_count INTEGER,                          -- Number of image runs
    status VARCHAR NOT NULL,                           -- 'success', 'failed', 'pending'
    error_message VARCHAR,                             -- Error details if status='failed'
    api_response_time_ms INTEGER,                      -- API response time for monitoring
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    -- Record creation timestamp
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,    -- Record last update timestamp
    PRIMARY KEY (embryo_id, focal_plane)
);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_embryo_images_location 
    ON gold.embryo_images_metadata(clinic_location);

CREATE INDEX IF NOT EXISTS idx_embryo_images_status 
    ON gold.embryo_images_metadata(status);

CREATE INDEX IF NOT EXISTS idx_embryo_images_extraction_timestamp 
    ON gold.embryo_images_metadata(extraction_timestamp);

-- Create a composite index for common queries
CREATE INDEX IF NOT EXISTS idx_embryo_images_location_status 
    ON gold.embryo_images_metadata(clinic_location, status);
