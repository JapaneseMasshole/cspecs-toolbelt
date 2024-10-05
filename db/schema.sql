DROP TABLE IF EXISTS instruments;
DROP TABLE IF EXISTS fields;
DROP TABLE IF EXISTS jobs;

-- Create table jobs
CREATE TABLE jobs (
    job_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_name TEXT NOT NULL,
    job_startdatetime INTEGER NOT NULL,
    duration INTEGER NOT NULL,
    job_status TEXT CHECK(job_status IN ('NOT STARTED', 'RUNNING', 'COMPLETED', 'FAILED')) NOT NULL
);

-- Create table instruments
CREATE TABLE instruments (
    instrument_name TEXT NOT NULL,
    job_id INTEGER,
    FOREIGN KEY (job_id) REFERENCES jobs(job_id),
    UNIQUE (instrument_name, job_id)
);

-- Create table fields
CREATE TABLE fields (
    field_name TEXT NOT NULL,
    job_id INTEGER,
    FOREIGN KEY (job_id) REFERENCES jobs(job_id),
    UNIQUE (field_name, job_id)
);