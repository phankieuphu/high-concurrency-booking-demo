-- Guard rails for every connection the application opens. Set on the role
-- (not as connection startup parameters) so they also apply through PgBouncer
-- in transaction-pooling mode, which rejects unknown startup parameters.
ALTER ROLE CURRENT_USER SET statement_timeout = '3s';
ALTER ROLE CURRENT_USER SET lock_timeout = '2s';
ALTER ROLE CURRENT_USER SET idle_in_transaction_session_timeout = '10s';
