-- Full database initialization: schema + seed data.
--
-- Veris runs a real Postgres and seeds it from this file via `schema_path` in
-- .veris/veris.yaml (SCHEMA_PATH=/agent/db/init.sql): the schema below, then the
-- seed users and cards.

CREATE TABLE users (
    id      TEXT PRIMARY KEY,
    name    TEXT NOT NULL,
    email   TEXT NOT NULL,
    phone   TEXT,            -- e.g. '+1-217-555-0101'
    address TEXT             -- full mailing address for card delivery
);

COMMENT ON TABLE users IS 'Bank customers who may hold one or more cards.';

CREATE TABLE cards (
    id         TEXT PRIMARY KEY,
    user_id    TEXT NOT NULL REFERENCES users(id),
    name       TEXT NOT NULL,                          -- cardholder name
    last4      TEXT NOT NULL,                          -- last 4 digits of card number
    type       TEXT NOT NULL CHECK (type   IN ('DEBIT', 'CREDIT', 'virtual')),
    status     TEXT NOT NULL DEFAULT 'active'
                             CHECK (status IN ('active', 'cancelled', 'frozen')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE cards IS 'Payment cards belonging to users. Status transitions: active → frozen → cancelled. A frozen card can be unfrozen back to active.';
COMMENT ON COLUMN cards.type   IS 'Card type: DEBIT, CREDIT, or virtual.';
COMMENT ON COLUMN cards.status IS 'Card status: active, cancelled, or frozen.';

CREATE INDEX idx_cards_user_id ON cards(user_id);
CREATE INDEX idx_cards_last4   ON cards(last4);

CREATE TABLE replacements (
    id                 TEXT PRIMARY KEY,
    card_id            TEXT NOT NULL REFERENCES cards(id),  -- the card this replacement is for
    new_last4          TEXT,                                -- last 4 digits of the replacement card
    reason             TEXT,                                -- why a replacement was issued: 'stolen', 'lost', or 'damaged'
    status             TEXT NOT NULL DEFAULT 'requested'
                             CHECK (status IN ('requested', 'mailed', 'delivered')),
    estimated_delivery TEXT,                                -- human-readable ETA, e.g. '14 business days'
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE replacements IS 'Replacement cards and their delivery lifecycle. A card has at most one in-flight replacement. Delivery status transitions: requested → mailed → delivered. When a delivered replacement is activated, a new active card is in use and the replacement is considered complete.';
COMMENT ON COLUMN replacements.card_id            IS 'The card that was replaced (look up the replacement by this card or by new_last4).';
COMMENT ON COLUMN replacements.status             IS 'Delivery status of the replacement card: requested, mailed, or delivered.';
COMMENT ON COLUMN replacements.new_last4          IS 'Last 4 digits of the replacement card being shipped.';

CREATE INDEX idx_replacements_card_id   ON replacements(card_id);
CREATE INDEX idx_replacements_new_last4 ON replacements(new_last4);

-- Seed users
INSERT INTO users (id, name, email, phone, address) VALUES
('u_alice_johnson',       'Alice Johnson',       'alice.johnson@example.com',       '+1-217-555-0101', '123 Market St, Springfield, IL'),
('u_bob_smith',           'Bob Smith',           'bob.smith@example.com',           '+1-212-555-0102', '456 Elm Ave, Metropolis, NY'),
('u_charlie_davis',       'Charlie Davis',       'charlie.davis@example.com',       '+1-616-555-0103', '789 Pine Rd, Lakeside, MI'),
('u_dana_lee',            'Dana Lee',            'dana.lee@example.com',            '+1-559-555-0104', '101 Cedar Blvd, Riverdale, CA'),
('u_elaine_nguyen',       'Elaine Nguyen',       'elaine.nguyen@example.com',       '+1-303-555-0105', '202 Oak Ct, Aurora, CO'),
('u_farhan_khan',         'Farhan Khan',         'farhan.khan@example.com',         '+1-919-555-0106', '303 Maple Ln, Raleigh, NC'),
('u_gabriela_rodriguez',  'Gabriela Rodriguez',  'gabriela.rodriguez@example.com',  '+1-602-555-0107', '404 Walnut St, Phoenix, AZ'),
('u_harlan_moore',        'Harlan Moore',        'harlan.moore@example.com',        '+1-608-555-0108', '505 Birch Way, Madison, WI'),
('u_isabella_perez',      'Isabella Perez',      'isabella.perez@example.com',      '+1-720-555-0109', '606 Spruce Dr, Denver, CO'),
('u_jamila_adams',        'Jamila Adams',        'jamila.adams@example.com',        '+1-404-555-0110', '707 Cherry St, Atlanta, GA'),
('u_kevin_brown',         'Kevin Brown',         'kevin.brown@example.com',         '+1-813-555-0111', '808 Palm Ave, Tampa, FL'),
('u_linh_tran',           'Linh Tran',           'linh.tran@example.com',           '+1-503-555-0112', '909 Magnolia Pl, Portland, OR'),
('u_marcelo_silva',       'Marcelo Silva',       'marcelo.silva@example.com',       '+1-512-555-0113', '1110 Cedar Ridge, Austin, TX'),
('u_nadia_petrov',        'Nadia Petrov',        'nadia.petrov@example.com',        '+1-206-555-0114', '1211 Aspen Loop, Seattle, WA'),
('u_oliver_wu',           'Oliver Wu',           'oliver.wu@example.com',           '+1-617-555-0115', '1312 Hemlock Ter, Boston, MA'),
('u_priya_sharma',        'Priya Sharma',        'priya.sharma@example.com',        '+1-408-555-0116', '1413 Juniper Ct, San Jose, CA'),
('u_quinn_martin',        'Quinn Martin',        'quinn.martin@example.com',        '+1-615-555-0117', '1514 Poplar Blvd, Nashville, TN'),
('u_ronan_foster',        'Ronan Foster',        'ronan.foster@example.com',        '+1-402-555-0118', '1615 Cypress Ln, Omaha, NE'),
('u_sanaa_ali',           'Sanaa Ali',           'sanaa.ali@example.com',           '+1-804-555-0119', '1716 Willow St, Richmond, VA'),
('u_tobias_clark',        'Tobias Clark',        'tobias.clark@example.com',        '+1-208-555-0120', '1817 Dogwood Dr, Boise, ID');

-- Seed cards
INSERT INTO cards (id, user_id, name, last4, type, status, created_at, updated_at) VALUES
('c_alice_debit',      'u_alice_johnson',      'Alice Johnson - Everyday Debit',       '1111', 'DEBIT',   'active',    '2025-01-01T09:00:00Z', '2025-03-20T12:00:00Z'),
('c_alice_credit',     'u_alice_johnson',      'Alice Johnson - Rewards Credit',       '2222', 'CREDIT',  'frozen',    '2025-01-15T09:00:00Z', '2025-02-22T08:30:00Z'),
('c_alice_virtual',    'u_alice_johnson',      'Alice Johnson - Virtual Card',         '3123', 'virtual', 'active',    '2025-03-10T17:15:00Z', '2025-03-10T17:15:00Z'),
('c_bob_debit',        'u_bob_smith',          'Bob Smith - Everyday Debit',           '3333', 'DEBIT',   'active',    '2025-02-10T08:45:00Z', '2025-02-10T08:45:00Z'),
('c_bob_travel',       'u_bob_smith',          'Bob Smith - Travel Credit',            '4444', 'CREDIT',  'active',    '2025-03-28T12:30:00Z', '2025-03-28T12:30:00Z'),
('c_charlie_debit',    'u_charlie_davis',      'Charlie Davis - Cash Debit',           '5555', 'DEBIT',   'active',    '2024-12-22T10:05:00Z', '2025-04-01T09:15:00Z'),
('c_dana_credit',      'u_dana_lee',           'Dana Lee - Platinum Credit',           '6666', 'CREDIT',  'active',    '2024-11-30T16:00:00Z', '2025-03-05T14:45:00Z'),
('c_dana_virtual',     'u_dana_lee',           'Dana Lee - Online Virtual',            '1077', 'virtual', 'active',    '2025-02-14T18:20:00Z', '2025-02-14T18:20:00Z'),
('c_elaine_debit',     'u_elaine_nguyen',      'Elaine Nguyen - Everyday Debit',       '7777', 'DEBIT',   'cancelled', '2024-10-19T07:30:00Z', '2025-02-02T11:00:00Z'),
('c_elaine_rewards',   'u_elaine_nguyen',      'Elaine Nguyen - Rewards Credit',       '8888', 'CREDIT',  'active',    '2025-01-08T09:55:00Z', '2025-03-12T16:40:00Z'),
('c_farhan_debit',     'u_farhan_khan',        'Farhan Khan - Checking Debit',         '9142', 'DEBIT',   'active',    '2024-09-27T13:10:00Z', '2025-01-29T10:25:00Z'),
('c_gabriela_credit',  'u_gabriela_rodriguez', 'Gabriela Rodriguez - Signature Credit','9900', 'CREDIT',  'frozen',    '2025-02-01T11:11:00Z', '2025-03-25T19:50:00Z'),
('c_gabriela_virtual', 'u_gabriela_rodriguez', 'Gabriela Rodriguez - Virtual Commerce','1084', 'virtual', 'active',    '2025-03-07T07:45:00Z', '2025-03-07T07:45:00Z'),
('c_harlan_debit',     'u_harlan_moore',       'Harlan Moore - Essential Debit',       '2201', 'DEBIT',   'active',    '2024-08-03T06:00:00Z', '2025-02-18T09:42:00Z'),
('c_isabella_debit',   'u_isabella_perez',     'Isabella Perez - Premium Debit',       '3412', 'DEBIT',   'active',    '2024-10-05T20:10:00Z', '2025-03-30T07:20:00Z'),
('c_isabella_cashback','u_isabella_perez',     'Isabella Perez - Cashback Credit',     '4523', 'CREDIT',  'active',    '2025-03-15T18:05:00Z', '2025-03-15T18:05:00Z'),
('c_jamila_credit',    'u_jamila_adams',       'Jamila Adams - Signature Credit',      '5634', 'CREDIT',  'active',    '2024-12-12T09:00:00Z', '2025-02-22T09:30:00Z'),
('c_kevin_debit',      'u_kevin_brown',        'Kevin Brown - Everyday Debit',         '6745', 'DEBIT',   'active',    '2024-11-09T08:15:00Z', '2025-03-02T11:05:00Z'),
('c_kevin_student',    'u_kevin_brown',        'Kevin Brown - Student Credit',         '7856', 'CREDIT',  'cancelled', '2024-12-24T13:55:00Z', '2025-01-30T10:10:00Z'),
('c_linh_debit',       'u_linh_tran',          'Linh Tran - Global Debit',             '8967', 'DEBIT',   'frozen',    '2025-01-02T22:15:00Z', '2025-03-19T14:55:00Z'),
('c_linh_virtual',     'u_linh_tran',          'Linh Tran - Virtual Card',             '9078', 'virtual', 'active',    '2025-03-02T08:40:00Z', '2025-03-02T08:40:00Z'),
('c_marcelo_debit',    'u_marcelo_silva',      'Marcelo Silva - Business Debit',       '0189', 'DEBIT',   'active',    '2024-09-15T10:20:00Z', '2025-03-01T10:20:00Z'),
('c_marcelo_business', 'u_marcelo_silva',      'Marcelo Silva - Business Credit',      '1290', 'CREDIT',  'active',    '2025-04-01T11:50:00Z', '2025-04-01T11:50:00Z'),
('c_nadia_travel',     'u_nadia_petrov',       'Nadia Petrov - Travel Credit',         '2301', 'CREDIT',  'active',    '2025-01-25T07:35:00Z', '2025-03-14T16:25:00Z'),
('c_oliver_debit',     'u_oliver_wu',          'Oliver Wu - Everyday Debit',           '3410', 'DEBIT',   'active',    '2024-11-11T09:00:00Z', '2025-02-01T09:45:00Z'),
('c_oliver_secure',    'u_oliver_wu',          'Oliver Wu - Secure Credit',            '4521', 'CREDIT',  'frozen',    '2025-02-18T15:30:00Z', '2025-03-18T20:00:00Z'),
('c_priya_credit',     'u_priya_sharma',       'Priya Sharma - Platinum Credit',       '5632', 'CREDIT',  'active',    '2024-10-10T10:45:00Z', '2025-03-11T11:10:00Z'),
('c_priya_travel',     'u_priya_sharma',       'Priya Sharma - Travel Rewards',        '6743', 'CREDIT',  'active',    '2025-01-14T08:20:00Z', '2025-03-26T13:35:00Z'),
('c_priya_virtual',    'u_priya_sharma',       'Priya Sharma - Virtual Card',          '7854', 'virtual', 'active',    '2025-02-20T19:05:00Z', '2025-02-20T19:05:00Z'),
('c_quinn_debit',      'u_quinn_martin',       'Quinn Martin - Everyday Debit',        '8965', 'DEBIT',   'active',    '2024-07-19T07:00:00Z', '2025-01-09T07:00:00Z'),
('c_ronan_debit',      'u_ronan_foster',       'Ronan Foster - Everyday Debit',        '9076', 'DEBIT',   'active',    '2024-09-02T12:15:00Z', '2025-02-14T12:15:00Z'),
('c_ronan_virtual',    'u_ronan_foster',       'Ronan Foster - Virtual Card',          '0187', 'virtual', 'active',    '2025-03-12T21:45:00Z', '2025-03-12T21:45:00Z'),
('c_sanaa_debit',      'u_sanaa_ali',          'Sanaa Ali - Everyday Debit',           '1298', 'DEBIT',   'frozen',    '2024-12-28T06:25:00Z', '2025-03-08T06:25:00Z'),
('c_sanaa_rewards',    'u_sanaa_ali',          'Sanaa Ali - Rewards Credit',           '2309', 'CREDIT',  'active',    '2025-01-22T17:45:00Z', '2025-03-22T09:05:00Z'),
('c_tobias_debit',     'u_tobias_clark',       'Tobias Clark - Everyday Debit',        '3418', 'DEBIT',   'active',    '2024-08-28T08:30:00Z', '2025-01-18T08:30:00Z');
