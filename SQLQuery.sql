/*
Global COVID-19 Deaths - Data Exploration

Explore the global data on confirmed COVID-19 deaths between 24 Feb 2020 and 16 December 2022
 
Data source: Our World in Data COVID-19 dataset

Performing the following tasks:
- Starting with location, date, total_cases, new_cases, total_deaths, population
- Total Cases vs Total Deaths in the United Kingdom
- Total Cases vs Population in the United Kingdom
- Countries with Highest Infection Rate compared to Population
- Countries with Highest Death Count per Population
- Using CTE to perform Calculation on Partition By in previous query
- Comparison between Total population and Covid Vaccinations
- Using Temp Table to perform Calculation on Partition By in previous query
- Creating View to store data for later visualizations

*/

-- Table by CovidDeaths
SELECT *
FROM AroinsightsProject.dbo.CovidDeaths
WHERE continent is not NULL
ORDER BY 3,4

-- Table by CovidVaccination
SELECT *
FROM AroinsightsProject.dbo.CovidVaccination
ORDER BY 3,4 DESC

-- Starting with location, date, total_cases, new_cases, total_deaths, population

SELECT location, date, total_cases, new_cases, total_deaths, population
FROM AroinsightsProject.dbo.CovidDeaths
WHERE continent is not NULL
ORDER BY 1, 2

-- Total Cases vs Total Deaths in the United Kingdom

SELECT location,date, total_cases, total_deaths, (total_deaths/total_cases)*100 as CovidDeathPercentage
FROM AroinsightsProject.dbo.CovidDeaths
WHERE location='United Kingdom' -- Change the location as per country
AND continent is not NULL
ORDER BY 1, 2 DESC

-- Total Cases vs Population in the United Kingdom

SELECT location, date, population, total_cases, (total_cases/population)*100 as CovidPercentPopulationInfected
FROM AroinsightsProject.dbo.CovidDeaths
WHERE location='United Kingdom' -- Change the location as per country
AND continent is not NULL
ORDER BY 1, 2

-- Countries with Highest Infection Rate compared to Population

SELECT location, population,Max(total_cases) as HighestInfectionCount, MAX(total_cases/population)*100 as CovidPercentPopulationInfected
FROM AroinsightsProject.dbo.CovidDeaths
--WHERE location='United Kingdom'
--AND continent is not NULL
GROUP BY location, population
ORDER BY CovidPercentPopulationInfected DESC

-- Countries with Highest Death Count per Population
-- CAST() converts as integer
-- SELECT location, Max(total_deaths) as TotalDeathCount
SELECT location, Max(CAST(total_deaths as INT)) as TotalDeathCount
FROM AroinsightsProject.dbo.CovidDeaths
--WHERE location='United Kingdom'
WHERE continent is not NULL
GROUP BY location
ORDER BY TotalDeathCount DESC

-- Continents with Highest Death Count per Population

SELECT continent, Max(CAST(total_deaths as INT)) as TotalDeathCount
FROM AroinsightsProject.dbo.CovidDeaths
--WHERE location='United Kingdom'
WHERE continent is not NULL
GROUP BY continent
ORDER BY TotalDeathCount DESC

-- Finding Global Death Rate by Percentage

SELECT SUM(new_cases) as TotalCases, SUM(CAST(new_deaths as INT)) as TotalDeaths, SUM(CAST(new_deaths as INT))/SUM(new_cases) * 100 as DeathPercentage
FROM AroinsightsProject.dbo.CovidDeaths
--WHERE location='United Kingdom'
WHERE continent is not NULL
ORDER BY 1, 2

-- Comparison between Total population and Covid Vaccinations
-- Using Join function to merge two tables

SELECT cd.continent, cd.location, cd.date, cd.population, cv.new_vaccinations, SUM(CONVERT(int,cv.new_vaccinations)) OVER (Partition by cd.Location Order by cd.location, cd.Date) as RollingPeopleVaccinated
FROM AroinsightsProject..CovidDeaths as cd
INNER JOIN AroinsightsProject..CovidVaccination as cv
ON cd.location = cv.location
and cd.date = cv.date
WHERE cd.continent is not NULL
ORDER BY 2, 3

--ORDER BY list of RANGE window frame has total size of 1020 bytes. Largest size supported is 900 bytes.
-- Using bigint, in order to Arithmetic overflow error converting expression to data type int. Warning: Null value is eliminated by an aggregate or other SET operation.

SELECT cd.continent, cd.location, cd.date, cd.population, cv.new_vaccinations, SUM(CONVERT(bigint,cv.new_vaccinations)) OVER (Partition by cd.Location Order by cd.location, cd.Date ROWS UNBOUNDED PRECEDING) as RollingPeopleVaccinated
FROM AroinsightsProject..CovidDeaths as cd
INNER JOIN AroinsightsProject..CovidVaccination as cv
ON cd.location = cv.location
and cd.date = cv.date
WHERE cd.continent is not NULL
ORDER BY 2, 3

-- Using CTE to perform Calculation on Partition By in previous query

With PopvsVac (Continent, Location, Date, Population, New_Vaccinations, RollingPeopleVaccinated)
as
(
SELECT cd.continent, cd.location, cd.date, cd.population, cv.new_vaccinations, SUM(CONVERT(bigint,cv.new_vaccinations)) OVER (Partition by cd.Location Order by cd.location, cd.Date ROWS UNBOUNDED PRECEDING) as RollingPeopleVaccinated
FROM AroinsightsProject..CovidDeaths as cd
INNER JOIN AroinsightsProject..CovidVaccination as cv
ON cd.location = cv.location
and cd.date = cv.date
WHERE cd.continent is not NULL
--order by 2,3
)
Select *, (RollingPeopleVaccinated/Population)*100
From PopvsVac

-- Using Temp Table to perform Calculation on Partition By in previous query

DROP Table if exists #PercentPopulationVaccinated
Create Table #PercentPopulationVaccinated
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime,
Population numeric,
New_vaccinations numeric,
RollingPeopleVaccinated numeric
)

Insert into #PercentPopulationVaccinated
Select cd.continent, cd.location, cd.date, cd.population, cv.new_vaccinations, SUM(Cast(cv.new_vaccinations as bigint)) OVER (Partition by cd.Location Order by cd.location, cd.Date ROWS UNBOUNDED PRECEDING) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From AroinsightsProject..CovidDeaths cd
Join AroinsightsProject..CovidVaccination cv
	On cd.location = cv.location
	and cd.date = cv.date

Select *, (RollingPeopleVaccinated/Population)*100
From #PercentPopulationVaccinated

-- Creating View to store data for later visualizations

Create View PercentPopulationVaccinated as
Select cd.continent, cd.location, cd.date, cd.population, cv.new_vaccinations
, SUM(CONVERT(int,cv.new_vaccinations)) OVER (Partition by cd.Location Order by cd.location, cd.Date ROWS UNBOUNDED PRECEDING) as RollingPeopleVaccinated
--, (RollingPeopleVaccinated/population)*100
From AroinsightsProject..CovidDeaths cd
Join AroinsightsProject..CovidVaccination cv
	On cd.location = cv.location
	and cd.date = cv.date
where cd.continent is not null