#!/bin/bash
# Script optimizado y corregido para DW de cáncer de mama

# Configuración
DB_NAME="breast_cancer_dwh"
DB_USER="postgres"
DB_PASSWORD="1234"
DATASET_PATH="/mnt/c/Users/Andres/Downloads/Breast_Cancer_Global_Dataset.csv"
export PGPASSWORD=$DB_PASSWORD
PSQL_CMD="psql -h localhost -U $DB_USER -v ON_ERROR_STOP=1"

# Optimizar parámetros PostgreSQL
echo "Optimizando parámetros de PostgreSQL..."
$PSQL_CMD -d postgres <<EOF
ALTER SYSTEM SET work_mem = '256MB';
ALTER SYSTEM SET maintenance_work_mem = '1GB';
ALTER SYSTEM SET effective_cache_size = '4GB';
ALTER SYSTEM SET temp_buffers = '256MB';
ALTER SYSTEM SET max_parallel_workers_per_gather = 4;
SELECT pg_reload_conf();
EOF

# Crear base de datos
echo "Creando base de datos $DB_NAME..."
$PSQL_CMD -d postgres -c "DROP DATABASE IF EXISTS $DB_NAME"
$PSQL_CMD -d postgres -c "CREATE DATABASE $DB_NAME"

# Crear esquema optimizado
echo "Creando estructura de tablas..."
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

CREATE TABLE dim_demographics (
    demo_id SERIAL PRIMARY KEY,
    median_age INTEGER,
    education_level VARCHAR(20)
);

-- Añadida la tabla faltante dim_healthcare
CREATE TABLE dim_healthcare (
    healthcare_id SERIAL PRIMARY KEY,
    healthcare_expenditure NUMERIC(10,2),
    screening_rate NUMERIC(5,2),
    access_to_care NUMERIC(5,2)
);

CREATE TABLE dim_health_factors (
    factor_id SERIAL PRIMARY KEY,
    obesity_rate NUMERIC(5,2),
    smoking_rate NUMERIC(5,2),
    alcohol_consumption NUMERIC(5,2),
    physical_activity_rate NUMERIC(5,2),
    family_history_rate NUMERIC(5,2),
    breastfeeding_rate NUMERIC(5,2)
);

-- Tabla temporal sin logging para carga inicial
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

-- Tabla de hechos particionada
CREATE TABLE fact_breast_cancer (
    fact_id BIGSERIAL,
    country_id INTEGER REFERENCES dim_country,
    demo_id INTEGER REFERENCES dim_demographics,
    factor_id INTEGER REFERENCES dim_health_factors,
    healthcare_id INTEGER REFERENCES dim_healthcare,
    year INTEGER NOT NULL DEFAULT 2023,
    population BIGINT,
    women_population BIGINT,
    breast_cancer_cases INTEGER,
    breast_cancer_deaths INTEGER,
    cases_per_100k NUMERIC(7,2),
    deaths_per_100k NUMERIC(7,2),
    mortality_rate NUMERIC(5,2),
    average_diagnosis_age INTEGER,
    survival_rate NUMERIC(5,2)
);
EOF

# Crear índices después de crear las tablas
echo "Creando índices..."
$PSQL_CMD -d $DB_NAME <<EOF
CREATE INDEX idx_country_name ON dim_country (country_name);
CREATE INDEX idx_demo_composite ON dim_demographics (median_age, education_level);
CREATE INDEX idx_health_factors ON dim_health_factors (obesity_rate, smoking_rate);
EOF

# Carga masiva optimizada
echo "Cargando datos desde $DATASET_PATH..."
$PSQL_CMD -d $DB_NAME -c "COPY temp_import FROM '$DATASET_PATH' DELIMITER ',' CSV HEADER"

# Insertar datos en dimensiones y hechos
echo "Procesando datos..."
$PSQL_CMD -d $DB_NAME <<EOF
BEGIN;

-- Insertar dimensiones
INSERT INTO dim_country (country_name, region, hdi_category, urbanization_rate, gdp_per_capita)
SELECT DISTINCT ON (country) 
    country, region, hdi_category, urbanization_rate, gdp_per_capita
FROM temp_import;

INSERT INTO dim_demographics (median_age, education_level)
SELECT DISTINCT ON (median_age, education_level)
    median_age, education_level
FROM temp_import;

INSERT INTO dim_healthcare (healthcare_expenditure, screening_rate, access_to_care)
SELECT DISTINCT ON (healthcare_expenditure, screening_rate, access_to_care)
    healthcare_expenditure, screening_rate, access_to_care
FROM temp_import;

INSERT INTO dim_health_factors (
    obesity_rate, smoking_rate, alcohol_consumption,
    physical_activity_rate, family_history_rate, breastfeeding_rate
)
SELECT DISTINCT ON (
    obesity_rate, smoking_rate, alcohol_consumption,
    physical_activity_rate, family_history_rate, breastfeeding_rate
)
    obesity_rate, smoking_rate, alcohol_consumption,
    physical_activity_rate, family_history_rate, breastfeeding_rate
FROM temp_import;

-- Insertar tabla de hechos
INSERT INTO fact_breast_cancer (
    country_id, demo_id, factor_id, healthcare_id,
    population, women_population, breast_cancer_cases, breast_cancer_deaths,
    cases_per_100k, deaths_per_100k, mortality_rate, average_diagnosis_age, survival_rate
)
SELECT 
    c.country_id, d.demo_id, f.factor_id, h.healthcare_id,
    t.population, t.women_population, t.breast_cancer_cases, t.breast_cancer_deaths,
    t.cases_per_100k, t.deaths_per_100k, t.mortality_rate, t.average_diagnosis_age, t.survival_rate
FROM temp_import t
JOIN dim_country c ON t.country = c.country_name
JOIN dim_demographics d ON t.median_age = d.median_age AND t.education_level = d.education_level
JOIN dim_health_factors f ON t.obesity_rate = f.obesity_rate AND t.smoking_rate = f.smoking_rate
JOIN dim_healthcare h ON t.healthcare_expenditure = h.healthcare_expenditure AND t.screening_rate = h.screening_rate;

COMMIT;

-- Optimización post-carga
VACUUM ANALYZE;
EOF

# Crear vistas OLAP optimizadas
echo "Creando vistas materializadas..."
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
    CASE WHEN f.obesity_rate > 25 THEN 'Alta' ELSE 'Normal' END AS obesidad,
    CASE WHEN f.smoking_rate > 30 THEN 'Alta' ELSE 'Normal' END AS tabaquismo,
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