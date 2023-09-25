/*
Covid 19 Data Exploration 

Skills used: Joins, CTE's, Temp Tables, Windows Functions, Aggregate Functions, Creating Views, Converting Data Types

*/

-- create table named covidDeaths 

create table covid_deaths (
	iso_code varchar(10),
	continent varchar(250),
	location varchar(250) ,
	date date,
	population real,
	total_cases real,
	new_cases real,
	new_cases_smoothed decimal,
	total_deaths real,
	new_deaths real,
	new_deaths_smoothed decimal,
	total_cases_per_million decimal,
	new_cases_per_million decimal,
	new_cases_smoothed_per_million decimal,
	total_deaths_per_million decimal,
	new_deaths_per_million decimal,
	new_deaths_smoothed_per_million decimal,
	reproduction_rate decimal ,
	icu_patients real,
	icu_patients_per_million decimal,
	hosp_patients real,
	hosp_patients_per_million decimal,
	weekly_icu_admissions decimal,
	weekly_icu_admissions_per_million decimal,
	weekly_hosp_admissions decimal,
	weekly_hosp_admissions_per_million decimal
);

--insert values into the covidDeaths table using a csv file

copy "covid_deaths" ("iso_code","continent","location","date","population","total_cases","new_cases","new_cases_smoothed","total_deaths","new_deaths","new_deaths_smoothed","total_cases_per_million","new_cases_per_million","new_cases_smoothed_per_million","total_deaths_per_million","new_deaths_per_million","new_deaths_smoothed_per_million","reproduction_rate","icu_patients","icu_patients_per_million","hosp_patients","hosp_patients_per_million","weekly_icu_admissions","weekly_icu_admissions_per_million","weekly_hosp_admissions","weekly_hosp_admissions_per_million")
from 'C:\Program Files\PostgreSQL\15\data\data_copy\CovidDeaths.csv' delimiter ',' CSV Header;

--selecting all values from coviddeaths table

select * from covid_deaths
order by 3,4;

-- create covidvaccinations table

create table covid_vaccinations (
	iso_code varchar(10),
	continent varchar(250) ,
	location varchar(250) ,
	date date,
	new_tests real,
	total_tests real,
	total_tests_per_thousand decimal,
	new_tests_per_thousand decimal,
	new_tests_smoothed real,
	new_tests_smoothed_per_thousand decimal,
	positive_rate decimal,
	tests_per_case decimal,
	tests_units varchar(100),
	total_vaccinations real,
	people_vaccinated real,
	people_fully_vaccinated real,
	new_vaccinations real,
	new_vaccinations_smoothed real,
	total_vaccinations_per_hundred decimal,
	people_vaccinated_per_hundred decimal,
	people_fully_vaccinated_per_hundred decimal,
	new_vaccinations_smoothed_per_million real
);

--insert values into covid_vaccinations table using csv file

copy "covid_vaccinations" ("iso_code","continent","location","date","new_tests","total_tests","total_tests_per_thousand","new_tests_per_thousand","new_tests_smoothed","new_tests_smoothed_per_thousand","positive_rate","tests_per_case","tests_units","total_vaccinations","people_vaccinated","people_fully_vaccinated","new_vaccinations","new_vaccinations_smoothed","total_vaccinations_per_hundred","people_vaccinated_per_hundred","people_fully_vaccinated_per_hundred","new_vaccinations_smoothed_per_million")
from 'C:\Program Files\PostgreSQL\15\data\data_copy\CovidVaccinations.csv' delimiter ',' csv header;

-- selecting values from covid_vaccinations table

select * from covid_vaccinations
order by 3,4;

--select data that we are going to work with

select Location, date, total_cases, new_cases, total_deaths, population
from covid_deaths
where continent is not null
order by 1,2;

--Total cases vs Total deaths
--show chances of dying if you catch covid in India

select Location, date, total_cases, total_deaths, cast(((total_deaths/total_cases)*100) as decimal(10,6)) as "Death Percentage"
from covid_deaths
where continent is not null and 
location='India'
order by 1,2;

--total cases vs population
--Shows what percentage of population got infected

select Location, date, total_cases, cast(population as bigint), cast(((total_cases/population)*100) as decimal(10,6)) as "PercentagePopulationInfected"
from covid_deaths
order by 1,2;

--Looking at countries with the highest infection rate in comparison to it's poulation
select Location, cast(population as bigint), max(cast(total_cases as bigint)) as HighestInfectionCount, max(cast((total_cases/population) as decimal (10,6)))*100  as "PercentagePopulationInfected"
from covid_deaths
where continent is not null and total_cases is not null
group by location, population
order by 4 desc ;

--Looking at countries with highest number of death in comparison to it's poulation

select Location, cast(population as bigint), max(cast(total_deaths as bigint)) as HighestDeathCount
from covid_deaths
where continent is not null and total_deaths is not null
group by location, population
order by 3 desc ;

--Total deaths continet wise

select continent, sum(cast(new_deaths as bigint)) as HighestDeathCount
from covid_deaths
where continent is not null
group by continent
order by 2 desc ;

-- Total Population vs Vaccinations

select dea.continent, dea.location, dea.date, cast(dea.population as bigint), vac.new_vaccinations,
sum(cast(vac.new_vaccinations as bigint))over(partition by dea.location order by dea.location, dea.date) as RollingPeopleVaccinated
from covid_deaths as dea
join covid_vaccinations as vac
on dea.location=vac.location
and dea.date=vac.date
where dea.continent is not null
order by 2;

--Using CTE to perform Calculation on RollingPeopleVaccinated in previous query

with PopvsVac (continent, location, date, population, new_vaccinations, RollingPeopleVaccinated)
as 
(
select dea.continent, dea.location, dea.date, cast(dea.population as bigint), vac.new_vaccinations,
sum(cast(vac.new_vaccinations as bigint))over(partition by dea.location order by dea.location, dea.date) as RollingPeopleVaccinated
from covid_deaths as dea
join covid_vaccinations as vac
on dea.location=vac.location
and dea.date=vac.date
where dea.continent is not null
)
select *,(RollingPeopleVaccinated/population)*100 as RollingPeopleVaccinatedPercentage from PopvsVac

-- Using Temp Table to perform Calculation on RollingPeopleVaccinated in previous query
drop VIEW if exists PeoplePopulationVaccinated;
create temp table PeoplePopulationVaccinated
(
	Location varchar(255),
	Date date,
	Population numeric,
	New_vaccinations numeric,
	RollingPeopleVaccinated numeric
)

insert into PeoplePopulationVaccinated
(
	select dea.location, dea.date, cast(dea.population as bigint), vac.new_vaccinations,
	sum(cast(vac.new_vaccinations as bigint))over(partition by dea.location order by dea.location, dea.date) as RollingPeopleVaccinated
	from covid_deaths as dea
	join covid_vaccinations as vac
	on dea.location=vac.location
	and dea.date=vac.date
	where dea.Location = 'India'
);

select *, (RollingPeopleVaccinated/population)*100 as RollingPeopleVaccinatedPercentage
from PeoplePopulationVaccinated
where date >= '2021-01-16';

--creating view to store data for future visualizations
create view PeoplePopulationVaccinated as
	(select dea.continent, dea.location, dea.date, cast(dea.population as bigint), vac.new_vaccinations,
	sum(cast(vac.new_vaccinations as bigint))over(partition by dea.location order by dea.location, dea.date) as RollingPeopleVaccinated
	from covid_deaths as dea
	join covid_vaccinations as vac
	on dea.location=vac.location
	and dea.date=vac.date
	where dea.continent is not null);

-- view showing chances of dying if you catch covid in India

create view DeathRateIndia as 
select Location, date, total_cases, total_deaths, cast(((total_deaths/total_cases)*100) as decimal(10,6)) as "Death Percentage"
from covid_deaths
where continent is not null and 
location='India'
order by 1,2;

--view of Total deaths continent wise

create view ContinentWiseTotalDeaths as
select continent, sum(cast(new_deaths as bigint)) as HighestDeathCount
from covid_deaths
where continent is not null
group by continent
order by 2 desc ;

-- view to see India's performance in handling covid in comparision to top 4 GDP countries i.e. USA, China, Japan and Germany

create view total_cases_vs_total_deaths as
select 
	location,
	date,
	sum(new_cases)over(partition by location order by location, date) as Rolling_Total_covid_cases,
	sum(new_deaths)over(partition by location order by location, date) as Rolling_Total_covid_deaths
from covid_deaths
where location in ('India', 'United States', 'China', 'Japan', 'Germany');

drop view total_cases_vs_total_deaths;

select * from covid_vaccinations
where location ='India';
	
--View for each countries population vaccination percentage 

create view country_vaccination_percentage as
select 
	vac.location,
	max(vac.date) as as_on,
	(max(vac.people_fully_vaccinated)/max(dea.population))*100 as vaccination_percentage
from covid_vaccinations as vac
inner join covid_deaths as dea
on dea.location=vac.location
group by vac.location
order by vac.location;

select * from country_vaccination_percentage;

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

