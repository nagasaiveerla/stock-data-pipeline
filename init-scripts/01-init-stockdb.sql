-- Create stock database and user
CREATE DATABASE stockdata;
CREATE USER stockuser WITH ENCRYPTED PASSWORD 'stockpass';
GRANT ALL PRIVILEGES ON DATABASE stockdata TO stockuser;

-- Connect to stockdata database
\c stockdata;

-- Grant schema permissions
GRANT ALL ON SCHEMA public TO stockuser;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO stockuser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO stockuser;

-- Create stock_data table
CREATE TABLE IF NOT EXISTS stock_data (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    open_price DECIMAL(10, 4),
    high_price DECIMAL(10, 4),
    low_price DECIMAL(10, 4),
    close_price DECIMAL(10, 4),
    volume BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, timestamp)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_stock_data_symbol ON stock_data(symbol);
CREATE INDEX IF NOT EXISTS idx_stock_data_timestamp ON stock_data(timestamp);
CREATE INDEX IF NOT EXISTS idx_stock_data_created_at ON stock_data(created_at);

-- Create a function to update the updated_at column
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Create trigger to automatically update updated_at
CREATE TRIGGER update_stock_data_updated_at 
    BEFORE UPDATE ON stock_data 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Grant permissions to stockuser
GRANT ALL PRIVILEGES ON TABLE stock_data TO stockuser;
GRANT ALL PRIVILEGES ON SEQUENCE stock_data_id_seq TO stockuser;