USE test_db;
GO

IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='users' AND xtype='U')
BEGIN
    CREATE TABLE users (
        id INT IDENTITY(1,1) PRIMARY KEY,
        name NVARCHAR(100) NOT NULL,
        email NVARCHAR(150) UNIQUE NOT NULL,
        created_at DATETIME2 DEFAULT GETDATE(),
        updated_at DATETIME2 DEFAULT GETDATE()
    );
END
GO

IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='posts' AND xtype='U')
BEGIN
    CREATE TABLE posts (
        id INT IDENTITY(1,1) PRIMARY KEY,
        user_id INT REFERENCES users(id) ON DELETE CASCADE,
        title NVARCHAR(200) NOT NULL,
        content NTEXT,
        published BIT DEFAULT 0,
        created_at DATETIME2 DEFAULT GETDATE(),
        updated_at DATETIME2 DEFAULT GETDATE()
    );
END
GO

-- Sample data
IF NOT EXISTS (SELECT * FROM users)
BEGIN
    INSERT INTO users (name, email) VALUES 
        ('John Doe', 'john@example.com'),
        ('Jane Smith', 'jane@example.com'),
        ('Bob Johnson', 'bob@example.com');
END
GO

IF NOT EXISTS (SELECT * FROM posts)
BEGIN
    INSERT INTO posts (user_id, title, content, published) VALUES 
        (1, 'First Post', 'This is my first post content.', 1),
        (1, 'Draft Post', 'This is a draft post.', 0),
        (2, 'Jane''s Post', 'Content from Jane.', 1),
        (3, 'Bob''s Thoughts', 'Some thoughts from Bob.', 1);
END
GO

