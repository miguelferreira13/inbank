/* 
    This is a SQL script to create the tables for the Netflix database. 
    The database is designed to store information about users, their subscriptions, and their activity on the platform.
 */

-- Create the users table
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subscription_id INTEGER NOT NULL,
    join_date DATE NOT NULL,
    last_payment_date DATE NOT NULL,
    age INTEGER NOT NULL,
    gender TEXT NOT NULL,
    device TEXT NOT NULL,
    active_profiles INTEGER NOT NULL,
    household_profile_ind INTEGER NOT NULL,
    FOREIGN KEY (subscription_id) REFERENCES subscriptions(id)
);

DELETE FROM users; -- Clear the table before inserting new data (assignment purposes only)

-- Create the subscriptions table
CREATE TABLE IF NOT EXISTS subscriptions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subscription_type TEXT NOT NULL,
    country TEXT NOT NULL,
    plan_duration INTEGER NOT NULL,
    monthly_revenue REAL NOT NULL,
    CONSTRAINT unique_subscription UNIQUE (subscription_type, country, plan_duration, monthly_revenue)
);

DELETE FROM subscriptions; -- Clear the table before inserting new data (assignment purposes only)

-- Create the activity table
CREATE TABLE IF NOT EXISTS activity (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    subscription_id INTEGER NOT NULL,
    movies_watched INTEGER NOT NULL,
    series_watched INTEGER NOT NULL,
    FOREIGN KEY (user_id) REFERENCES users(id),
    FOREIGN KEY (subscription_id) REFERENCES subscriptions(id),
    CONSTRAINT unique_user_subscription UNIQUE (user_id, subscription_id)
);

DELETE FROM activity; -- Clear the table before inserting new data (assignment purposes only)
