CREATE TABLE info_governance (
    id SERIAL PRIMARY KEY,  -- Unique identifier for each record
    ticker_id INTEGER REFERENCES ticker(id),  -- Foreign key to ticker table
    date TIMESTAMP,  -- Timestamp to track when the data was recorded
    audit_risk INTEGER,  -- Audit risk
    board_risk INTEGER,  -- Board risk
    compensation_risk INTEGER,  -- Compensation risk
    shareholder_rights_risk INTEGER,  -- Shareholder rights risk
    overall_risk INTEGER,  -- Overall risk
    governance_epoch_date DATE,  -- Governance epoch date
    compensation_as_of_epoch_date DATE  -- Compensation as of epoch date
);
