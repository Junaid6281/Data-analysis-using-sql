-- DATA CLEANING 

select * from layoffs;

-- remove duplicates
-- standardise the data
-- null values
-- remove columns that are not neccessary


-- remove duplicates
Create table layoffs_staging
like layoffs;

select * from layoffs_staging;

insert layoffs_staging select * from layoffs;

select *,
Row_number() over(
partition by company, location,industry, total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions)
as row_num 
from layoffs_staging;

with duplicate_CTE as (
select *,
Row_number() over(
partition by company, location,industry, total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions)
as row_num 
from layoffs_staging
)
select * from duplicate_CTE
where row_num >1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


insert into layoffs_staging2
select *,
Row_number() over(
partition by company, location,industry, total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions)
as row_num 
from layoffs_staging;

DELETE from layoffs_staging2
where row_num > 1;

select * from layoffs_staging2;

-- standardizing data 

select DISTINCT (TRIM(company)) from layoffs_staging2;

select company from layoffs_staging2;

update layoffs_staging2 set company = TRIM(company);

select distinct industry from layoffs_staging2
order by 1;

select * from layoffs_staging2 where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct location from layoffs_staging2 order by 1;

select distinct (trim(country)) from layoffs_staging2 order by 1;

update layoffs_staging2
set country  = trim(country);

select distinct country from layoffs_staging2 order by 1; 

select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`,'%m/%d/%Y');

select * from layoffs_staging2;

Alter table layoffs_staging2
modify column `date` DATE;

-- removing nulls or populating null values with similar data;

select * from layoffs_staging2 where industry = '' OR industry is null;

select t1.company,t2.company,t1.industry,t2.industry from layoffs_staging2 t1
JOIN layoffs_staging2 t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2
set industry = NULL
where industry = '';

update layoffs_staging2 t1
JOIN layoffs_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select * from layoffs_staging2 where company = 'Airbnb';

-- delete column and rows
select * from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;
delete  from layoffs_staging2 where total_laid_off is null and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;

