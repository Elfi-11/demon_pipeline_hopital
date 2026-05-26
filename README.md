# Démo Pipeline as Code - Hôpital

Stack :
- MinIO : dépôt des CSV bruts
- PostgreSQL : tables `service` et `patient`
- Airflow : ingestion + nettoyage + insertion

## Lancer

```bash
docker compose up -d
```

## Interfaces

- Airflow : http://localhost:8080  
  login : `admin`  
  password : `admin`

- MinIO : http://localhost:9001  
  login : `minioadmin`  
  password : `minioadmin`

- PostgreSQL :
  - host : `localhost`
  - port : `5432`
  - database : `hospital_db`
  - user : `hospital_user`
  - password : `hospital_pwd`

## Démo

1. Lancer la stack.
2. Aller dans MinIO et vérifier le bucket `hospital-raw`.
3. Aller dans Airflow.
4. Lancer le DAG `ingest_patients_from_minio`.
5. Vérifier les données dans PostgreSQL.

## Vérification SQL

```bash
docker exec -it demo_postgres_hospital psql -U hospital_user -d hospital_db
```

Puis :

```sql
SELECT * FROM service ORDER BY id;

SELECT
    p.id,
    p.nom,
    p.prenom,
    p.age,
    p.pathologie,
    s.nom AS service,
    p.source_file
FROM patient p
JOIN service s ON s.id = p.service_id
ORDER BY p.id;
```

## Reset complet

```bash
docker compose down -v
docker compose up -d
```
