#!/bin/bash
# Script optimizado para DW de cáncer de mama con grandes datasets

# Configuración
DB_NAME="breast_cancer_dwh"
DB_USER="postgres"
DB_PASSWORD="1234"
DATASET_PATH="/ruta/completa/al/archivo.csv"
export PGPASSWORD=$DB_PASSWORD
PSQL_CMD="psql -h localhost -U $DB_USER -v ON_ERROR_STOP=1"

# Optimizar parámetros PostgreSQL
$PSQL_CMD -d postgres <<EOF
ALTER SYSTEM SET work_mem = '256MB';
ALTER SYSTEM SET maintenance_work_mem = '2GB';
ALTER SYSTEM SET effective_cache_size = '4GB';
ALTER SYSTEM SET temp_buffers = '256MB';
ALTER SYSTEM SET max_parallel_workers_per_gather = 4;
SELECT pg_reload_conf();
EOF

# Crear base de datos
echo "Creando base de datos..."
$PSQL_CMD -d postgres -c "DROP DATABASE IF EXISTS $DB_NAME"
$PSQL_CMD -d postgres -c "CREATE DATABASE $DB_NAME"

# Crear esquema optimizado
$PSQL_CMD -d $DB_NAME <<EOF
-- Tablas de dimensiones con índices
CREATE TABLE dim_country (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL,
    region VARCHAR(50),
    hdi_category VARCHAR(20),
    urbanization_rate NUMERIC(5,2),
    gdp_per_capita NUMERIC(10,2)
);
CREATE INDEX idx_country_name ON dim_country (country_name);

CREATE TABLE dim_demographics (
    demo_id SERIAL PRIMARY KEY,
    median_age INTEGER,
    education_level VARCHAR(20)
);
CREATE INDEX idx_demo_composite ON dim_demographics (median_age, education_level);

CREATE TABLE dim_health_factors (
    factor_id SERIAL PRIMARY KEY,
    obesity_rate NUMERIC(5,2),
    smoking_rate NUMERIC(5,2),
    alcohol_consumption NUMERIC(5,2),
    physical_activity_rate NUMERIC(5,2),
    family_history_rate NUMERIC(5,2),
    breastfeeding_rate NUMERIC(5,2)
);
CREATE INDEX idx_health_factors ON dim_health_factors USING BRIN (obesity_rate, smoking_rate);

-- Tabla de hechos particionada
CREATE TABLE fact_breast_cancer (
    fact_id BIGSERIAL,
    country_id INTEGER REFERENCES dim_country,
    demo_id INTEGER REFERENCES dim_demographics,
    factor_id INTEGER REFERENCES dim_health_factors,
    healthcare_id INTEGER REFERENCES dim_healthcare,
    year INTEGER NOT NULL,
    population BIGINT,
    women_population BIGINT,
    breast_cancer_cases INTEGER,
    breast_cancer_deaths INTEGER,
    cases_per_100k NUMERIC(7,2),
    deaths_per_100k NUMERIC(7,2),
    mortality_rate NUMERIC(5,2),
    average_diagnosis_age INTEGER,
    survival_rate NUMERIC(5,2)
) PARTITION BY RANGE (year);

CREATE TABLE fact_breast_cancer_2023 PARTITION OF fact_breast_cancer
FOR VALUES FROM (2023) TO (2024);

-- Tabla temporal sin logging
CREATE UNLOGGED TABLE temp_import (
    country VARCHAR(100),
    region VARCHAR(50),
    hdi_category VARCHAR(20),
    population BIGINT,
    screening_rate NUMERIC(5,2),
    median_age INTEGER,
    urbanization_rate NUMERIC(5,2),
    healthcare_expenditure NUMERIC(10,2),
    gdp_per_capita NUMERIC(10,2),
    obesity_rate NUMERIC(5,2),
    smoking_rate NUMERIC(5,2),
    alcohol_consumption NUMERIC(5,2),
    physical_activity_rate NUMERIC(5,2),
    family_history_rate NUMERIC(5,2),
    breastfeeding_rate NUMERIC(5,2),
    average_diagnosis_age INTEGER,
    survival_rate NUMERIC(5,2),
    access_to_care NUMERIC(5,2),
    education_level VARCHAR(20),
    women_population BIGINT,
    breast_cancer_cases INTEGER,
    breast_cancer_deaths INTEGER,
    cases_per_100k NUMERIC(7,2),
    deaths_per_100k NUMERIC(7,2),
    mortality_rate NUMERIC(5,2)
);
EOF

# Carga masiva optimizada
echo "Cargando datos..."
$PSQL_CMD -d $DB_NAME -c "COPY temp_import FROM '$DATASET_PATH' DELIMITER ',' CSV HEADER"

# Insertar datos en dimensiones y hechos en transacciones
$PSQL_CMD -d $DB_NAME <<EOF
BEGIN;

-- Insertar dimensiones con DISTINCT ON
INSERT INTO dim_country (country_name, region, hdi_category, urbanization_rate, gdp_per_capita)
SELECT DISTINCT ON (country)
    country, region, hdi_category, urbanization_rate, gdp_per_capita
FROM temp_import;

INSERT INTO dim_demographics (median_age, education_level)
SELECT DISTINCT ON (median_age, education_level)
    median_age, education_level
FROM temp_import;

INSERT INTO dim_health_factors (
    obesity_rate, smoking_rate, alcohol_consumption,
    physical_activity_rate, family_history_rate, breastfeeding_rate
)
SELECT DISTINCT ON (
    obesity_rate, smoking_rate, alcohol_consumption,
    physical_activity_rate, family_history_rate, breastfeeding_rate
) *
FROM temp_import;

-- Insertar tabla de hechos en lote
INSERT INTO fact_breast_cancer
SELECT
    c.country_id, d.demo_id, f.factor_id, h.healthcare_id,
    2023, t.population, t.women_population, t.breast_cancer_cases,
    t.breast_cancer_deaths, t.cases_per_100k, t.deaths_per_100k,
    t.mortality_rate, t.average_diagnosis_age, t.survival_rate
FROM temp_import t
JOIN dim_country c ON t.country = c.country_name
JOIN dim_demographics d ON t.median_age = d.median_age AND t.education_level = d.education_level
JOIN dim_health_factors f ON t.obesity_rate = f.obesity_rate
    AND t.smoking_rate = f.smoking_rate
    AND t.alcohol_consumption = f.alcohol_consumption
JOIN dim_healthcare h ON t.healthcare_expenditure = h.healthcare_expenditure
    AND t.screening_rate = h.screening_rate;

COMMIT;

-- Optimizar tablas después de carga
VACUUM ANALYZE;
CLUSTER fact_breast_cancer USING fact_breast_cancer_pkey;
EOF

# Crear vistas OLAP optimizadas con índices
$PSQL_CMD -d $DB_NAME <<EOF
CREATE MATERIALIZED VIEW mv_mortality_analysis AS
SELECT 
    c.region,
    c.hdi_category,
    COUNT(*) AS total_paises,
    SUM(f.breast_cancer_cases) AS casos_totales,
    SUM(f.breast_cancer_deaths) AS muertes_totales,
    AVG(f.mortality_rate) AS tasa_mortalidad_promedio
FROM fact_breast_cancer f
JOIN dim_country c USING (country_id)
GROUP BY ROLLUP(c.region, c.hdi_category);

CREATE INDEX idx_mv_mortality ON mv_mortality_analysis (region, hdi_category);

CREATE MATERIALIZED VIEW mv_risk_factor_analysis AS
SELECT 
    CASE 
        WHEN f.obesity_rate > 25 THEN 'Alta' 
        ELSE 'Normal' 
    END AS obesidad,
    CASE 
        WHEN f.smoking_rate > 30 THEN 'Alta' 
        ELSE 'Normal' 
    END AS tabaquismo,
    AVG(fc.cases_per_100k) AS incidencia,
    AVG(fc.mortality_rate) AS mortalidad
FROM fact_breast_cancer fc
JOIN dim_health_factors f USING (factor_id)
GROUP BY CUBE(obesidad, tabaquismo);

CREATE INDEX idx_mv_risk ON mv_risk_factor_analysis (obesidad, tabaquismo);

-- Mantenimiento final
REINDEX DATABASE $DB_NAME;
EOF

echo "Proceso completado exitosamente!"