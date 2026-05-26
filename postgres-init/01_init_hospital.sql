CREATE TABLE IF NOT EXISTS service (
    id SERIAL PRIMARY KEY,
    nom VARCHAR(120) NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS patient (
    id SERIAL PRIMARY KEY,
    nom VARCHAR(120) NOT NULL,
    prenom VARCHAR(120) NOT NULL,
    age INTEGER NOT NULL CHECK (age >= 0 AND age <= 130),
    pathologie TEXT NOT NULL,
    service_id INTEGER NOT NULL REFERENCES service(id),
    source_file VARCHAR(255),
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (nom, prenom, age, pathologie, service_id)
);

INSERT INTO service (nom) VALUES
    ('Chirurgie cardiovasculaire'),
    ('Orthopédie'),
    ('Pédiatrie'),
    ('Gynécologie'),
    ('Neurologie'),
    ('Endocrinologie'),
    ('Cardiologie'),
    ('Urgences')
ON CONFLICT (nom) DO NOTHING;
