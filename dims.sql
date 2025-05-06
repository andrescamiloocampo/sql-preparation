create table Dim_Person (
    id serial primary key not null,
    ACCESS_TO_CARE double precision not null,
    EDUCATION_LEVEL varchar(255) not null
);

create table Dim_Region (
    id serial primary key not null,    
    REGION varchar(255) not null,
    URBANIZATION_RATE double precision not null,
    GDP_PER_CAPITA double precision not null,
);

create table Dim_Population (
    id serial primary key not null,
    HEALTHCARE_EXPENDITURE double precision not null,
    SURVIVAL_RATE double precision not null,
    BREAST_CANCER_CASES integer not null,
    BREAST_CANCER_DEATHS integer not null
);

create table Fact_cancerPerRegion (
    id serial primary key not null,
    PERSON_ID integer not null,
    REGION_ID integer not null,
    POPULATION_ID integer not null,
    CONSTRAINT fk_person
        FOREIGN KEY(PERSON_ID)
            REFERENCES Dim_Person(id),
    CONSTRAINT fk_region
        FOREIGN KEY(REGION_ID)
            REFERENCES Dim_Region(id),
    CONSTRAINT fk_population
        FOREIGN KEY(POPULATION_ID)
            REFERENCES Dim_Population(id)
);