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
5. Lancer le DAG `validate_patient_age_distribution_gx` (Great Expectations — distribution d'âge).
6. Vérifier les données dans PostgreSQL.

### Qualité des données — âge (Great Expectations)

- DAG : `validate_patient_age_distribution_gx`
- Librairie : [Great Expectations](https://greatexpectations.io/) (open source)
- Contrôles : bornes 0–130, moyenne plausible, distribution par tranches vs référence (divergence KL)
- Rapport JSON : `gx/reports/age_distribution_latest.json` (histogramme + résultat des expectations, pour Grafana ou revue manuelle)

```bash
# Relire le dernier rapport depuis l'hôte
type gx\reports\age_distribution_latest.json
```

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
