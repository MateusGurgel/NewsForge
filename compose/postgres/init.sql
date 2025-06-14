CREATE TABLE news
(
    id            SERIAL PRIMARY KEY,
    title         TEXT NOT NULL,
    origin        TEXT NOT NULL,
    resume        TEXT NOT NULL,
    transcription TEXT NOT NULL,
    collected_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE image
(
    id      SERIAL PRIMARY KEY,
    url     VARCHAR(258) NOT NULL,
    news_id INT,
    FOREIGN KEY (news_id) REFERENCES news (id)
);

INSERT INTO news (title, origin, resume, transcription)
VALUES ('Exemplo de notícia', 'Example Origin', 'Example Resume', 'Example Transcription');

CREATE USER batch WITH PASSWORD 'batchbatch';
REVOKE ALL ON SCHEMA public FROM batch;
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM batch;
ALTER DEFAULT PRIVILEGES IN SCHEMA public REVOKE ALL ON TABLES FROM batch;
GRANT INSERT ON TABLE news TO batch;