-- Table to store static additional information about each company
CREATE TABLE info_additional (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to link with the ticker table
    underlying_symbol VARCHAR(10),  -- Underlying symbol
    short_name VARCHAR(50),  -- Short name
    long_name VARCHAR(100),  -- Long name
    first_trade_date_epoch_utc BIGINT,  -- First trade date in epoch UTC
    time_zone_full_name VARCHAR(50),  -- Full name of the time zone
    time_zone_short_name VARCHAR(10),  -- Short name of the time zone
    uuid UUID,  -- UUID for the record
    message_board_id VARCHAR(50),  -- Message board ID
    gmt_offset_milliseconds BIGINT,  -- GMT offset in milliseconds

    -- IR Information
    ir_website VARCHAR(100),  -- IR website
    max_age INTEGER,  -- Max age

    -- Company Description and Employees
    full_time_employees INTEGER  -- Full-time employees
);
