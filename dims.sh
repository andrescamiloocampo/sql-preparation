#!/bin/bash
# Script para crear un data warehouse para análisis de cáncer de mama

# Configuración
DB_NAME="breast_cancer_dwh"
DB_USER="postgres" # Cambia esto según tu configuración
DB_PASSWORD="1234"
export PGPASSWORD=$DB_PASSWORD
PSQL_CMD="sudo psql -h localhost -U $DB_USER"

echo "Iniciando creación del Data Warehouse para análisis de cáncer de mama..."

# Crear la base de datos
echo "Creando base de datos $DB_NAME..."
dropdb -h localhost -U $DB_USER --if-exists $DB_NAME
createdb -h localhost -U $DB_USER $DB_NAME
# Crear las tablas de dimensiones y hechos
$PSQL_CMD -d $DB_NAME << EOF

-- Crear tabla de dimensión País/Región
CREATE TABLE dim_country (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(100),
    region VARCHAR(50),
    hdi_category VARCHAR(20),
    urbanization_rate NUMERIC(5,2),
    gdp_per_capita NUMERIC(10,2)
);

-- Crear tabla de dimensión Demografía
CREATE TABLE dim_demographics (
    demo_id SERIAL PRIMARY KEY,
    median_age INTEGER,
    education_level VARCHAR(20)
);

-- Crear tabla de dimensión Factores de Salud
CREATE TABLE dim_health_factors (
    factor_id SERIAL PRIMARY KEY,
    obesity_rate NUMERIC(5,2),
    smoking_rate NUMERIC(5,2),
    alcohol_consumption NUMERIC(5,2),
    physical_activity_rate NUMERIC(5,2),
    family_history_rate NUMERIC(5,2),
    breastfeeding_rate NUMERIC(5,2)
);

-- Crear tabla de dimensión Sistema de Salud
CREATE TABLE dim_healthcare (
    healthcare_id SERIAL PRIMARY KEY,
    healthcare_expenditure NUMERIC(10,2),
    screening_rate NUMERIC(5,2),
    access_to_care NUMERIC(5,2)
);

-- Crear tabla de hechos para Estadísticas de Cáncer de Mama
CREATE TABLE fact_breast_cancer (
    fact_id SERIAL PRIMARY KEY,
    country_id INTEGER REFERENCES dim_country(country_id),
    demo_id INTEGER REFERENCES dim_demographics(demo_id),
    factor_id INTEGER REFERENCES dim_health_factors(factor_id),
    healthcare_id INTEGER REFERENCES dim_healthcare(healthcare_id),
    year INTEGER DEFAULT 2023,  -- Asumimos los datos son de 2023
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

-- Crear índices para mejorar rendimiento
CREATE INDEX idx_fact_country ON fact_breast_cancer(country_id);
CREATE INDEX idx_fact_demo ON fact_breast_cancer(demo_id);
CREATE INDEX idx_fact_factors ON fact_breast_cancer(factor_id);
CREATE INDEX idx_fact_healthcare ON fact_breast_cancer(healthcare_id);

EOF

echo "Tablas creadas correctamente."

# Procesar el archivo CSV y cargar datos
echo "Cargando datos desde $DATASET..."

# Convertir el CSV a formato psql-compatible si es necesario
# (por ejemplo, si contiene encabezados o tiene formato incorrecto)
# cat $DATASET | tail -n +2 > temp_data.csv

# Función para cargar datos en las tablas de dimensiones y obtener IDs
$PSQL_CMD -d $DB_NAME << EOF

-- Crear tabla temporal para facilitar la carga
CREATE TEMP TABLE temp_data (
    Country VARCHAR(100),
    Region VARCHAR(50),
    HDI_Category VARCHAR(20),
    Population BIGINT,
    Screening_Rate NUMERIC(5,2),
    Median_Age INTEGER,
    Urbanization_Rate NUMERIC(5,2),
    Healthcare_Expenditure NUMERIC(10,2),
    GDP_Per_Capita NUMERIC(10,2),
    Obesity_Rate NUMERIC(5,2),
    Smoking_Rate NUMERIC(5,2),
    Alcohol_Consumption NUMERIC(5,2),
    Physical_Activity_Rate NUMERIC(5,2),
    Family_History_Rate NUMERIC(5,2),
    Breastfeeding_Rate NUMERIC(5,2),
    Average_Diagnosis_Age INTEGER,
    Survival_Rate NUMERIC(5,2),
    Access_To_Care NUMERIC(5,2),
    Education_Level VARCHAR(20),
    Women_Population BIGINT,
    Breast_Cancer_Cases INTEGER,
    Breast_Cancer_Deaths INTEGER,
    Cases_Per_100K NUMERIC(7,2),
    Deaths_Per_100K NUMERIC(7,2),
    Mortality_Rate NUMERIC(5,2)
);

-- Cargar datos desde el archivo temporal
COPY temp_data FROM '/mnt/c/Users/Andres/Downloads/Breast_Cancer_Global_Dataset.csv' DELIMITER ',' CSV HEADER;

-- Insertar datos en las dimensiones
-- dim_country
INSERT INTO dim_country (country_name, region, hdi_category, urbanization_rate, gdp_per_capita)
SELECT DISTINCT Country, Region, HDI_Category, Urbanization_Rate, GDP_Per_Capita 
FROM temp_data;

-- dim_demographics
INSERT INTO dim_demographics (median_age, education_level)
SELECT DISTINCT Median_Age, Education_Level
FROM temp_data;

-- dim_health_factors
INSERT INTO dim_health_factors (obesity_rate, smoking_rate, alcohol_consumption, 
                              physical_activity_rate, family_history_rate, breastfeeding_rate)
SELECT DISTINCT Obesity_Rate, Smoking_Rate, Alcohol_Consumption, 
              Physical_Activity_Rate, Family_History_Rate, Breastfeeding_Rate
FROM temp_data;

-- dim_healthcare
INSERT INTO dim_healthcare (healthcare_expenditure, screening_rate, access_to_care)
SELECT DISTINCT Healthcare_Expenditure, Screening_Rate, Access_To_Care
FROM temp_data;

-- Insertar datos en la tabla de hechos uniendo con las dimensiones
INSERT INTO fact_breast_cancer (
    country_id, demo_id, factor_id, healthcare_id,
    population, women_population, breast_cancer_cases, breast_cancer_deaths,
    cases_per_100k, deaths_per_100k, mortality_rate, average_diagnosis_age, survival_rate
)
SELECT 
    c.country_id, d.demo_id, f.factor_id, h.healthcare_id,
    t.Population, t.Women_Population, t.Breast_Cancer_Cases, t.Breast_Cancer_Deaths,
    t.Cases_Per_100K, t.Deaths_Per_100K, t.Mortality_Rate, t.Average_Diagnosis_Age, t.Survival_Rate
FROM 
    temp_data t
JOIN 
    dim_country c ON t.Country = c.country_name
JOIN 
    dim_demographics d ON t.Median_Age = d.median_age AND t.Education_Level = d.education_level
JOIN 
    dim_health_factors f ON 
        t.Obesity_Rate = f.obesity_rate AND 
        t.Smoking_Rate = f.smoking_rate AND
        t.Alcohol_Consumption = f.alcohol_consumption AND
        t.Physical_Activity_Rate = f.physical_activity_rate AND
        t.Family_History_Rate = f.family_history_rate AND
        t.Breastfeeding_Rate = f.breastfeeding_rate
JOIN 
    dim_healthcare h ON 
        t.Healthcare_Expenditure = h.healthcare_expenditure AND
        t.Screening_Rate = h.screening_rate AND
        t.Access_To_Care = h.access_to_care;

-- Eliminar tabla temporal
DROP TABLE temp_data;

EOF

echo "Datos cargados correctamente."

# Crear consultas OLAP y vistas materializadas
echo "Creando consultas OLAP y vistas materializadas..."

$PSQL_CMD -d $DB_NAME << EOF

-- ROLLUP: Analizando mortalidad por región y categoría HDI
CREATE MATERIALIZED VIEW mv_mortality_rollup AS
SELECT 
    c.region,
    c.hdi_category,
    COUNT(*) AS countries,
    SUM(f.breast_cancer_cases) AS total_cases,
    SUM(f.breast_cancer_deaths) AS total_deaths,
    ROUND(AVG(f.mortality_rate), 2) AS avg_mortality_rate
FROM 
    fact_breast_cancer f
JOIN 
    dim_country c ON f.country_id = c.country_id
GROUP BY 
    ROLLUP(c.region, c.hdi_category);

-- CUBE: Análisis multidimensional de factores de riesgo, región y educación
CREATE MATERIALIZED VIEW mv_risk_factors_cube AS
SELECT 
    c.region,
    d.education_level,
    CASE 
        WHEN h.obesity_rate > 25 THEN 'Alta'
        WHEN h.obesity_rate > 15 THEN 'Media'
        ELSE 'Baja'
    END AS obesity_category,
    CASE 
        WHEN h.smoking_rate > 30 THEN 'Alta'
        WHEN h.smoking_rate > 20 THEN 'Media'
        ELSE 'Baja'
    END AS smoking_category,
    COUNT(*) AS countries,
    ROUND(AVG(f.cases_per_100k), 2) AS avg_incidence,
    ROUND(AVG(f.mortality_rate), 2) AS avg_mortality
FROM 
    fact_breast_cancer f
JOIN 
    dim_country c ON f.country_id = c.country_id
JOIN 
    dim_demographics d ON f.demo_id = d.demo_id
JOIN 
    dim_health_factors h ON f.factor_id = h.factor_id
GROUP BY 
    CUBE(c.region, d.education_level, 
         CASE 
             WHEN h.obesity_rate > 25 THEN 'Alta'
             WHEN h.obesity_rate > 15 THEN 'Media'
             ELSE 'Baja'
         END,
         CASE 
             WHEN h.smoking_rate > 30 THEN 'Alta'
             WHEN h.smoking_rate > 20 THEN 'Media'
             ELSE 'Baja'
         END);

-- GROUPING SETS: Análisis específico de grupos de interés
CREATE MATERIALIZED VIEW mv_healthcare_analysis AS
SELECT 
    c.region,
    c.hdi_category,
    CASE 
        WHEN hc.screening_rate > 60 THEN 'Alta'
        WHEN hc.screening_rate > 30 THEN 'Media'
        ELSE 'Baja'
    END AS screening_category,
    CASE 
        WHEN hc.healthcare_expenditure > 5000 THEN 'Alto'
        WHEN hc.healthcare_expenditure > 2000 THEN 'Medio'
        ELSE 'Bajo'
    END AS expenditure_category,
    COUNT(*) AS countries,
    ROUND(AVG(f.survival_rate), 2) AS avg_survival_rate,
    ROUND(AVG(f.cases_per_100k), 2) AS avg_incidence
FROM 
    fact_breast_cancer f
JOIN 
    dim_country c ON f.country_id = c.country_id
JOIN 
    dim_healthcare hc ON f.healthcare_id = hc.healthcare_id
GROUP BY 
    GROUPING SETS (
        (c.region),
        (c.hdi_category),
        (CASE 
            WHEN hc.screening_rate > 60 THEN 'Alta'
            WHEN hc.screening_rate > 30 THEN 'Media'
            ELSE 'Baja'
         END),
        (CASE 
            WHEN hc.healthcare_expenditure > 5000 THEN 'Alto'
            WHEN hc.healthcare_expenditure > 2000 THEN 'Medio'
            ELSE 'Bajo'
         END),
        (c.region, c.hdi_category),
        (c.region, CASE 
                      WHEN hc.screening_rate > 60 THEN 'Alta'
                      WHEN hc.screening_rate > 30 THEN 'Media'
                      ELSE 'Baja'
                   END),
        (c.hdi_category, CASE 
                            WHEN hc.healthcare_expenditure > 5000 THEN 'Alto'
                            WHEN hc.healthcare_expenditure > 2000 THEN 'Medio'
                            ELSE 'Bajo'
                         END)
    );

-- Vista materializada para análisis de edad de diagnóstico y supervivencia
CREATE MATERIALIZED VIEW mv_age_survival_analysis AS
SELECT 
    c.region,
    c.hdi_category,
    CASE 
        WHEN f.average_diagnosis_age < 50 THEN 'Temprana'
        WHEN f.average_diagnosis_age < 60 THEN 'Media'
        ELSE 'Tardía'
    END AS diagnosis_age_category,
    COUNT(*) AS countries,
    ROUND(AVG(f.survival_rate), 2) AS avg_survival_rate,
    ROUND(AVG(f.cases_per_100k), 2) AS avg_incidence,
    ROUND(AVG(f.mortality_rate), 2) AS avg_mortality
FROM 
    fact_breast_cancer f
JOIN 
    dim_country c ON f.country_id = c.country_id
GROUP BY 
    GROUPING SETS (
        (c.region),
        (c.hdi_category),
        (CASE 
            WHEN f.average_diagnosis_age < 50 THEN 'Temprana'
            WHEN f.average_diagnosis_age < 60 THEN 'Media'
            ELSE 'Tardía'
         END),
        (c.region, c.hdi_category),
        (c.region, CASE 
                      WHEN f.average_diagnosis_age < 50 THEN 'Temprana'
                      WHEN f.average_diagnosis_age < 60 THEN 'Media'
                      ELSE 'Tardía'
                   END),
        (c.hdi_category, CASE 
                            WHEN f.average_diagnosis_age < 50 THEN 'Temprana'
                            WHEN f.average_diagnosis_age < 60 THEN 'Media'
                            ELSE 'Tardía'
                         END)
    );

-- Crear índices para las vistas materializadas
CREATE INDEX idx_mv_mortality_region ON mv_mortality_rollup(region);
CREATE INDEX idx_mv_mortality_hdi ON mv_mortality_rollup(hdi_category);

CREATE INDEX idx_mv_risk_region ON mv_risk_factors_cube(region);
CREATE INDEX idx_mv_risk_education ON mv_risk_factors_cube(education_level);
CREATE INDEX idx_mv_risk_obesity ON mv_risk_factors_cube(obesity_category);
CREATE INDEX idx_mv_risk_smoking ON mv_risk_factors_cube(smoking_category);

CREATE INDEX idx_mv_healthcare_region ON mv_healthcare_analysis(region);
CREATE INDEX idx_mv_healthcare_hdi ON mv_healthcare_analysis(hdi_category);
CREATE INDEX idx_mv_healthcare_screening ON mv_healthcare_analysis(screening_category);
CREATE INDEX idx_mv_healthcare_expenditure ON mv_healthcare_analysis(expenditure_category);

CREATE INDEX idx_mv_age_region ON mv_age_survival_analysis(region);
CREATE INDEX idx_mv_age_hdi ON mv_age_survival_analysis(hdi_category);
CREATE INDEX idx_mv_age_diagnosis ON mv_age_survival_analysis(diagnosis_age_category);

EOF

echo "Consultas OLAP y vistas materializadas creadas exitosamente."
echo "Data Warehouse para análisis de cáncer de mama completado."

# Limpieza de archivos temporales
rm -f temp_data.csv

# Fin del script
exit 0