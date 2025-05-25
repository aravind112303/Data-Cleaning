                                          -- DATA CLEANING --

select * from layoffs;

-- 1.REMOVE DUPLICATES
-- 2.STANDARDIZE THE DATA
-- 3.NULL VALUES OR BLANK VALUES
-- 4.REMOVE ANY COLUMNS

CREATE table layoffs_staging
like layoffs;

select * from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

-- 1.REMOVE DUPLICATES

with duplicate_cte as 
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date' , stage, country, funds_raised_millions ) as row_num
from layoffs_staging
)
select * 
from duplicate_cte 
where row_num > 1;

-- check dupliicate 

select *
from layoffs_staging
where company = 'Casper';

-- to delete, create an other staging2

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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- now we got empty table with field names
select * from layoffs_staging2;

-- now insert the all values into staging2
-- then delete the duplicates

insert into layoffs_staging2
select *,
row_number() over(
partition by company, location, industry, total_laid_off, percentage_laid_off, 'date' , stage, country, funds_raised_millions ) as row_num
from layoffs_staging;

DELETE FROM layoffs_staging2
WHERE row_num > 1;

-- 2.STANDARDIZE THE DATA
-- ( it is to finding issues and fixing it )
-- trim() takes off whitespaces at front and end

update layoffs_staging2
set company = trim(company);

-- now lets look at industry 

select *
from layoffs_staging2
where industry like 'Crypto%';

update layoffs_staging2
set industry = 'Crypto'
where industry like 'Crypto%';

-- we use trailing because of . in unitedstates country so updated it

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country);

-- now set date column from text to date
 
 select `date`
 from layoffs_staging2;
 
 update layoffs_staging2
 set `date` = str_to_date(`date`,'%m/%d/%Y');
 
 -- still date definition is text so do
 
 alter table layoffs_staging2
 modify column `date` DATE;
 
 -- 3.NULL VALUES OR BLANK VALUES

select  t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
   on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2
set industry = null
where industry = '';

update layoffs_staging2 t1
join layoffs_staging2 t2
  on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select * from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

delete from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null;

-- 4.REMOVE ANY COLUMNS

alter table layoffs_staging2
drop column row_num;

