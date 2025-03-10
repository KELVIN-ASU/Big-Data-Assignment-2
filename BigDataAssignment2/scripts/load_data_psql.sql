-- Terminate active connections before dropping database
\c postgres;
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = 'ecommerce' AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS ecommerce;
CREATE DATABASE ecommerce;
\c ecommerce;

-- Create tables
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'campaigns') THEN
        CREATE TABLE campaigns (
            id NUMERIC PRIMARY KEY,
            campaign_type VARCHAR(50),
            channel VARCHAR(50),
            topic VARCHAR(255),
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            total_count NUMERIC,
            ab_test BOOLEAN,
            warmup_mode BOOLEAN,
            hour_limit NUMERIC,
            subject_length NUMERIC,
            subject_with_personalization BOOLEAN,
            subject_with_deadline BOOLEAN,
            subject_with_emoji BOOLEAN,
            subject_with_bonuses BOOLEAN,
            subject_with_discount BOOLEAN,
            subject_with_saleout BOOLEAN,
            is_test BOOLEAN,
            position NUMERIC
        );
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'client_first_purchase') THEN
        CREATE TABLE client_first_purchase (
            client_id BIGINT PRIMARY KEY,
            first_purchase_date DATE,
            user_id BIGINT UNIQUE,
            user_device_id NUMERIC
        );
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'events') THEN
        CREATE TABLE events (
            event_id SERIAL PRIMARY KEY,
            event_time TIMESTAMP,
            event_type VARCHAR(50),
            product_id NUMERIC,
            category_id NUMERIC,
            category_code VARCHAR(255),
            brand VARCHAR(255),
            price NUMERIC,
            user_id BIGINT,
            user_session UUID,
            FOREIGN KEY (user_id) REFERENCES client_first_purchase(user_id) ON DELETE CASCADE
        );
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'friends') THEN
        CREATE TABLE friends (
            friend1 BIGINT,
            friend2 BIGINT,
            PRIMARY KEY (friend1, friend2),
            FOREIGN KEY (friend1) REFERENCES client_first_purchase(user_id) ON DELETE CASCADE,
            FOREIGN KEY (friend2) REFERENCES client_first_purchase(user_id) ON DELETE CASCADE
        );
    END IF;
END $$;

DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'messages') THEN
        CREATE TABLE messages (
            id SERIAL PRIMARY KEY,
            message_id UUID UNIQUE,
            campaign_id NUMERIC,
            message_type VARCHAR(50),
            client_id BIGINT,
            channel VARCHAR(50),
            category VARCHAR(50),
            platform VARCHAR(50),
            email_provider VARCHAR(255),
            stream VARCHAR(50),
            date DATE,
            sent_at TIMESTAMP,
            is_opened BOOLEAN,
            opened_first_time_at TIMESTAMP,
            opened_last_time_at TIMESTAMP,
            is_clicked BOOLEAN,
            clicked_first_time_at TIMESTAMP,
            clicked_last_time_at TIMESTAMP,
            is_unsubscribed BOOLEAN,
            unsubscribed_at TIMESTAMP,
            is_hard_bounced BOOLEAN,
            hard_bounced_at TIMESTAMP,
            is_soft_bounced BOOLEAN,
            soft_bounced_at TIMESTAMP,
            is_complained BOOLEAN,
            complained_at TIMESTAMP,
            is_blocked BOOLEAN,
            blocked_at TIMESTAMP,
            is_purchased BOOLEAN,
            purchased_at TIMESTAMP,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            user_device_id NUMERIC,
            user_id BIGINT,
            FOREIGN KEY (campaign_id) REFERENCES campaigns(id) ON DELETE CASCADE,
            FOREIGN KEY (client_id) REFERENCES client_first_purchase(client_id) ON DELETE CASCADE
        );
    END IF;
END $$;

-- Load data from CSV files using temp tables to handle duplicates
CREATE TEMP TABLE temp_campaigns AS TABLE campaigns;
COPY temp_campaigns FROM 'C:/Users/Administrator/Desktop/BigDataAssignment2/Cleaned_Datasets/campaigns_cleaned.csv' DELIMITER ',' CSV HEADER;
INSERT INTO campaigns SELECT * FROM temp_campaigns ON CONFLICT DO NOTHING;
DROP TABLE temp_campaigns;

CREATE TEMP TABLE temp_client_first_purchase AS TABLE client_first_purchase;
COPY temp_client_first_purchase FROM 'C:/Users/Administrator/Desktop/BigDataAssignment2/Cleaned_Datasets/client_first_purchase_cleaned.csv' DELIMITER ',' CSV HEADER;
INSERT INTO client_first_purchase SELECT * FROM temp_client_first_purchase ON CONFLICT DO NOTHING;
DROP TABLE temp_client_first_purchase;

CREATE TEMP TABLE temp_events AS TABLE events;
COPY temp_events(event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session) FROM 'C:/Users/Administrator/Desktop/BigDataAssignment2/Cleaned_Datasets/events_cleaned.csv' DELIMITER ',' CSV HEADER;
INSERT INTO events(event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session) 
SELECT event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session FROM temp_events ON CONFLICT DO NOTHING;
DROP TABLE temp_events;

CREATE TEMP TABLE temp_friends AS TABLE friends;
COPY temp_friends FROM 'C:/Users/Administrator/Desktop/BigDataAssignment2/Cleaned_Datasets/friends_cleaned.csv' DELIMITER ',' CSV HEADER;
INSERT INTO friends(friend1, friend2) SELECT friend1, friend2 FROM temp_friends WHERE friend1 IN (SELECT user_id FROM client_first_purchase) AND friend2 IN (SELECT user_id FROM client_first_purchase) ON CONFLICT DO NOTHING;
DROP TABLE temp_friends;

CREATE TEMP TABLE temp_messages AS TABLE messages;
COPY temp_messages FROM 'C:/Users/Administrator/Desktop/BigDataAssignment2/Cleaned_Datasets/messages_cleaned.csv' DELIMITER ',' CSV HEADER;
INSERT INTO messages SELECT * FROM temp_messages WHERE client_id IN (SELECT client_id FROM client_first_purchase) ON CONFLICT DO NOTHING;
DROP TABLE temp_messages;
