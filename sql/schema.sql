CREATE TABLE IF NOT EXISTS posts (
    id          BIGINT       NOT NULL PRIMARY KEY,
    title       VARCHAR(255) NOT NULL,
    like_count  BIGINT       NOT NULL DEFAULT 0
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS likes (
    post_id     BIGINT      NOT NULL,
    user_id     BIGINT      NOT NULL,
    created_at  DATETIME(3) NOT NULL,
    PRIMARY KEY (post_id, user_id),
    KEY idx_likes_user (user_id),
    CONSTRAINT fk_likes_post FOREIGN KEY (post_id) REFERENCES posts (id)
) ENGINE=InnoDB;

-- Seed 100 posts (ids 1..100)
INSERT IGNORE INTO posts (id, title)
WITH RECURSIVE seq (n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 100)
SELECT n, CONCAT('Post #', n) FROM seq;
