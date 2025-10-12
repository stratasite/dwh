-- SQLite initialization script
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    title VARCHAR(200) NOT NULL,
    content TEXT,
    published BOOLEAN DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sample data
INSERT INTO users (id, name, email) VALUES
    (1, 'John Doe', 'john@example.com'),
    (2, 'Jane Smith', 'jane@example.com'),
    (3, 'Bob Johnson', 'bob@example.com');

INSERT INTO posts (id, user_id, title, content, published) VALUES
    (1, 1, 'First Post', 'This is my first post content.', 1),
    (2, 1, 'Draft Post', 'This is a draft post.', 0),
    (3, 2, 'Jane''s Post', 'Content from Jane.', 1),
    (4, 3, 'Bob''s Thoughts', 'Some thoughts from Bob.', 1);
