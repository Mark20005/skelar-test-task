-- separate schema creation
CREATE SCHEMA platform;
-- table creation
CREATE TABLE platform.users_sessions (
    id SERIAL PRIMARY KEY,
    id_user INT NOT NULL,
    action VARCHAR(10) CHECK (action IN ('open', 'close')),
    date_action TIMESTAMP NOT NULL
);
-- inserting dummy data
INSERT INTO platform.users_sessions (id_user, action, date_action)
VALUES
(1, 'open',  NOW() - INTERVAL '9 days' + INTERVAL '08:00:00'),
(1, 'close', NOW() - INTERVAL '9 days' + INTERVAL '09:30:00'),
(1, 'open',  NOW() - INTERVAL '7 days' + INTERVAL '10:00:00'),
(1, 'close', NOW() - INTERVAL '7 days' + INTERVAL '12:15:00'),
(1, 'open',  NOW() - INTERVAL '2 days' + INTERVAL '08:00:00'),
(1, 'close', NOW() - INTERVAL '2 days' + INTERVAL '10:15:00'),

(2, 'open',  NOW() - INTERVAL '3 days' + INTERVAL '13:00:00'),
(2, 'close', NOW() - INTERVAL '3 days' + INTERVAL '15:00:00'),
(2, 'open',  NOW() - INTERVAL '1 day' + INTERVAL '10:00:00'),
(2, 'close', NOW() - INTERVAL '1 day' + INTERVAL '12:30:00'),

(3, 'open',  NOW() - INTERVAL '3 hours'),

(4, 'open',  NOW() - INTERVAL '1 day' + INTERVAL '22:00:00'),

(5, 'open',  NOW() - INTERVAL '2 days' + INTERVAL '18:30:00'),

(6, 'open',  NOW() - INTERVAL '5 days' + INTERVAL '09:15:00'),
(6, 'close', NOW() - INTERVAL '5 days' + INTERVAL '11:45:00'),
(6, 'open',  NOW() - INTERVAL '2 days' + INTERVAL '14:00:00'),
(6, 'close', NOW() - INTERVAL '2 days' + INTERVAL '15:30:00'),

(7, 'open', NOW() - INTERVAL '1 hour');
