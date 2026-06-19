-- ClickHouse initialisation for DWH system tests

CREATE TABLE IF NOT EXISTS test_db.users (
    id       UInt32,
    name     String,
    email    String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;

CREATE TABLE IF NOT EXISTS test_db.posts (
    id         UInt32,
    user_id    UInt32,
    title      String,
    content    String,
    published  UInt8,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;

INSERT INTO test_db.users (id, name, email) VALUES
    (1, 'John Doe',    'john@example.com'),
    (2, 'Jane Smith',  'jane@example.com'),
    (3, 'Bob Johnson', 'bob@example.com');

INSERT INTO test_db.posts (id, user_id, title, content, published) VALUES
    (1, 1, 'First Post',     'This is my first post content.', 1),
    (2, 1, 'Draft Post',     'This is a draft post.',          0),
    (3, 2, 'Jane''s Post',   'Content from Jane.',             1),
    (4, 3, 'Bob''s Thoughts','Some thoughts from Bob.',        1);
