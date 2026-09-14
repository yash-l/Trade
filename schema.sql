auth_sessions (
    client_code VARCHAR(50) NOT NULL,
    token TEXT NOT NULL,
    expiry TIMESTAMP WITH TIME ZONE NOT NULL,
    metadata JSONB,
    CONSTRAINT auth_sessions_pk PRIMARY KEY (client_code)
);

CREATE INDEX IF NOT EXISTS idx_auth_expiry ON auth_sessions (expiry);
