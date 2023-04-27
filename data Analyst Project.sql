
/*CREATE TABLES */
CREATE TABLE covid_vaccinations(iso_code VARCHAR(20),continent VARCHAR(30),location VARCHAR(50),
date date,new_tests INT,total_tests INT,
total_tests_per_thousand DECIMAL(7,3),new_tests_per_thousand DECIMAL(7,3), new_tests_smoothed INT,
new_tests_smoothed_per_thousand DECIMAL (6,3),positive_rate DECIMAL(4,3),tests_per_case FLOAT,tests_unit VARCHAR(30),
total_vaccinations INT,people_vaccinated INT,people_fully_vaccinated INT,new_vaccinations INT,new_vaccinations_smoothed INT,
total_vaccinations_per_hundred DECIMAL(5,2),people_vaccinated_per_hundred DECIMAL(5,2),
people_fully_vaccinated_per_hundred DECIMAL(4,2),new_vaccinations_smoothed_per_million INT,stringency_index FLOAT,
population_density DECIMAL(8,3),median_age DECIMAL(3,1),aged_65_older DECIMAL(5,3),aged_70_older DECIMAL(5,3),
gdp_per_capital FLOAT,extreme_poverty DECIMAL(3,1),cadiovasc_death_rate DECIMAL(6,3),diabetes_prevalence FLOAT,
female_smokers FLOAT,male_smokers FLOAT,handwashing_facilities DECIMAL(5,3),hospital_beds_per_thousand FLOAT,
life_expectancy DECIMAL(4,2),human_development_index DECIMAL(4,3)); 
/*CREATE TABLE */
CREATE TABLE covid_deaths(iso_code VARCHAR(10),continent VARCHAR(15),location VARCHAR(50),date date,total_cases INT,
new_cases INT,new_cases_smoothed FLOAT,total_deaths INT,new_deaths INT,new_deaths_smoothed FLOAT,
total_cases_per_million DECIMAL(9,3),new_cases_per_million DECIMAL(7,3),new_cases_smoothed_per_million DECIMAL(7,3),
total_deaths_per_million DECIMAL(7,3),new_deaths_per_million DECIMAL(6,3),new_deaths_smoothed_per_million DECIMAL(5,3),
reproduction_rate DECIMAL(3,2),icu_patients INT,icu_patients_per_million DECIMAL(6,3),hosp_patients INT,
hosp_patients_per_million DECIMAL(7,3),weekly_icu_admissions FLOAT,weekly_icu_admissions_per_million DECIMAL(5,2),
weekly_hosp_admissions FLOAT,weekly_hosp_admissions_per_million DECIMAL(7,3),new_tests INT,total_tests INT,
total_tests_per_thousand DECIMAL(7,3),new_tests_per_thousand DECIMAL(7,3),new_tests_smoothed INT,
new_tests_smoothed_per_thousand DECIMAL(6,3),positive_rate DECIMAL(4,3),tests_per_case FLOAT,
tests_units VARCHAR(30),total_vaccinations INT,people_vaccinated INT,people_fully_vaccinated INT,
new_vaccinations INT,new_vaccinations_smoothed INT,total_vaccinations_per_hundred DECIMAL(5,2),
people_vaccinated_per_hundred FLOAT,people_fully_vaccinated_per_hundred DECIMAL(4,2),
new_vaccinations_smoothed_per_million INT,stringency_index FLOAT,population BIGINT,population_density DECIMAL(8,3),
median_age FLOAT,aged_65_older DECIMAL(5,3),aged_70_older DECIMAL(5,3),gdp_per_capita FLOAT,extreme_poverty FLOAT,
cadiovasc_death_rate DECIMAL(6,3),diabetes_prevalence DECIMAL(4,2),female_smokers DECIMAL(7,3),male_smoker DECIMAL(3,1),
handwashing_facilities DECIMAL(5,3),hospital_beds_per_thousand DECIMAL(4,2),life_expectancy DECIMAL(4,2),
human_development_index DECIMAL(4,3));
/*Importing Large data into covid_vaccinations table */
LOAD DATA INFILE 'covid_vaccinations.csv' INTO TABLE covid_vaccinations
FIELDS TERMINATED BY ','
IGNORE 1 LINES;
/* Importing to Covid_deaths table */
LOAD DATA INFILE 'covid_deaths.csv' INTO TABLE covid_deaths
FIELDS TERMINATED BY ','
IGNORE 1 LINES;
/* Counting no of rows */
SELECT COUNT(*)
FROM covid_deaths;
/*To select all data on Covid_deaths*/
SELECT *
FROM covid_deaths
WHERE continent IS NULL;
/* To select all data on Covid_vaccinations */
SELECT *
FROM covid_vaccinations
ORDER BY location,date;
/* To select specified data*/
SELECT location,date,total_cases,new_cases,total_deaths
FROM covid_deaths
ORDER BY location,date;
/*To select specified data from covid-vaccinations */
SELECT location,date,total_cases,new_cases,total_deaths
FROM covid_vaccinations
ORDER BY location,date;
/*Total deaths statictics in New Cases */
SELECT location,total_deaths,total_cases,(total_deaths / total_cases) * 100 as deathPerc
FROM covid_deaths
GROUP BY location,total_deaths,total_cases;
/* To get death staticstics in Africa */
SELECT location,total_deaths,total_cases,(total_deaths / total_cases) * 100 as deathPerc
FROM covid_deaths
WHERE location LIKE '%africa%'
GROUP BY location,total_deaths,total_cases;
/* looking at total cases vs population*/
SELECT location,date,total_cases,population,(total_cases/population) * 100 as CasesPerc
FROM covid_deaths
WHERE location LIKE '%africa%'
GROUP BY location,date,total_cases,population;
/*countries with the highest infection rate compared to population*/
SELECT location,MAX(total_cases) AS highinfect,population,MAX(total_cases/population) * 100 as CasesPerc
FROM covid_deaths
GROUP BY location,population
ORDER BY CasesPerc DESC;
/*showing countries with highest death count*/
SELECT location, MAX(total_deaths) AS deathdata
FROM covid_deaths
WHERE continent IS NOT NULL 
GROUP BY location
ORDER BY deathdata DESC ;

SELECT * FROM covid_deaths
WHERE location LIKE '%wORLD%'
AND continent IS NOT NULL;


/*To Find percentage of new cases in total cases among covid_death data*/
SELECT location,new_cases,total_cases,(new_cases/total_cases) * 100 AS newperc
FROM covid_deaths;
/* Looking at total deaths among total cases in each location */
SELECT continent,total_cases, total_deaths, (total_deaths/total_cases) * 100 AS perc, SUM(total_cases) OVER() AS sum
FROM covid_deaths
ORDER BY perc;
/*Looking at countries with highest infection in each continent compared to populations*/
SELECT population,continent,MAX(total_cases) OVER(PARTITION BY continent) AS highest,(total_cases/population) * 100 AS totalperc
FROM covid_deaths
ORDER BY continent DESC;
/*showing location with highest death count per population*/
SELECT location, MAX(total_deaths) OVER(partition by location) AS highest_death_count, population 
FROM covid_deaths
WHERE continent IS NOT NULL;
/* Looking at total_cases Vs Populations*/
SELECT location, continent, Population, total_cases, (total_cases/population) * 100 As covid_rate
FROM covid_deaths 
GROUP BY population,total_cases,location,continent
ORDER BY covid_rate DESC;
/*Looking at countries with highest infection_rate */
WITH CTEs AS (SELECT location, date, Population, MAX(total_cases) AS inf, (total_cases/population) * 100 As covid_rate
FROM covid_deaths 
GROUP BY population,total_cases,location,date
ORDER BY covid_rate DESC)
SELECT location,MAX(covid_rate) AS covid_surge,population
FROM CTEs
GROUP BY population,location;
/*Showing countries death rate in highly infected region*/
WITH CTES AS
(SELECT location,MAX(total_cases) AS highlyinfected,population,(total_deaths/total_cases) * 100 AS covid_death
FROM covid_deaths 
GROUP BY location,population,(total_deaths/total_cases) * 100 
ORDER BY covid_death DESC)
SELECT location,population,MAX(covid_death) AS mortality_rate
FROM CTEs
GROUP BY location,population
ORDER BY mortality_rate DESC;
/*LET'S BREAK THINGS DOWN BY CONTINENT
*/
SELECT continent,MAX(total_deaths) AS deathsurge 
FROM covid_deaths
WHERE continent <> "NULL" 
GROUP BY continent;

/* To know the total number of deaths */
SELECT continent,SUM(total_deaths) AS deathstatistics
FROM covid_deaths
WHERE continent <> "NULL"
GROUP BY continent;

SELECT COUNT(*)
FROM covid_Vaccinations;

SELECT * 
FROM covid_deaths
WHERE location LIKE '%World%';

/*GLOBAL NUMBERS - daily information about death rate */
WITH subquery AS (SELECT date, SUM(new_cases) AS breeding_rate,SUM(new_deaths) AS death_rate
FROM covid_deaths
WHERE continent <> "NULL"
GROUP BY date)
SELECT date,breeding_rate/death_rate AS deathperc
FROM subquery
ORDER BY deathperc DESC;
/* aggregated data of death rate in the world */
WITH subquery AS (SELECT SUM(new_deaths) AS death_rate,SUM(new_cases) AS breeding_rate
FROM covid_deaths)
SELECT death_rate,breeding_rate,(death_rate/breeding_rate) AS deathperc
FROM subquery;

/* QUERYING COVID TABLE */
SELECT *
FROM covid_deaths AS d
LEFT JOIN covid_vaccinations AS v
ON d.iso_code = v.iso_code;

/* Looking at total vaccination VS population */
WITH subquery AS (SELECT c.continent AS continent,d.date AS date, d.location AS location,
c.new_vaccinations AS vaccinated,d.population AS census
 FROM covid_vaccinations AS c
INNER JOIN covid_deaths AS d
ON c.location = d.location
AND c.iso_code = d.iso_code
AND c.date = d.date)
SELECT continent,location,vaccinated,date,
SUM(vaccinated) OVER(partition by location) AS total,census, (vaccinated/census) AS vaccine_perc
FROM subquery
WHERE continent <> "NULL"
ORDER BY total DESC;

/*Getting running total of total_deaths on each location */
SELECT SUM(total_deaths) OVER(PARTITION BY continent ORDER BY total_deaths ROWS BETWEEN UNBOUNDED PRECEDING
AND CURRENT ROW) AS running_totals,continent,total_deaths
FROM covid_deaths
WHERE continent <> "NULL";
/*Creating tempoary table */
CREATE TEMPORARY TABLE updated_table AS 
(SELECT SUM(total_deaths) OVER(PARTITION BY continent ORDER BY total_deaths ROWS BETWEEN UNBOUNDED PRECEDING
AND CURRENT ROW) AS running_totals,continent,total_deaths
FROM covid_deaths
WHERE continent <> "NULL");
/* Querying the temporary table */
SELECT *
FROM updated_table;
/*Creating View */
CREATE VIEW virtual_table 
AS (SELECT continent,SUM(total_deaths) AS deathstatistics
FROM covid_deaths
WHERE continent <> "NULL"
GROUP BY continent);


