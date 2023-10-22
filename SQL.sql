USE Rinha;
CREATE TABLE users_entity (
    id INTEGER PRIMARY KEY,
    nome VARCHAR(35) NOT NULL,
    apelido VARCHAR(35) NOT NULL,
    nascimento DATE NOT NULL,
    stack CITEXT NOT NULL,
    agregado TEXT NOT NULL
);
