-- Exploratory Data Analysis

select * from layoffs_staging2;

select max(total_laid_off) , max(percentage_laid_off)
from layoffs_staging2;

select * 
from layoffs_staging2
where percentage_laid_off = 1
order by total_laid_off desc;

select * 
from layoffs_staging2
where percentage_laid_off = 1
order by funds_raised_millions desc;

select company, sum(total_laid_off)
from layoffs_staging2
group by company
order by 2 desc;

select industry, sum(total_laid_off)
from layoffs_staging2
group by industry
order by 2 desc;

select country, sum(total_laid_off)
from layoffs_staging2
group by country
order by 2 desc;

select min(`date`) , max(`date`)
from layoffs_staging2;

select `date`, sum(total_laid_off)
from layoffs_staging2
group by `date`
order by 1 desc;

select year(`date`), sum(total_laid_off)
from layoffs_staging2
group by year(`date`)
order by 1 desc;

select substring(`date`,6,2) as `month` , sum(total_laid_off)
from layoffs_staging2
group by substring(`date`,6,2)
order by 2 desc;

select substring(`date`,1,7) as `month` , sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc;

select stage, sum(total_laid_off)
from layoffs_staging2
group by stage
order by 2 desc;

select company, year(`date`) as `year`, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by company, year(`date`)
order by 3 desc;

with company_year as 
(
select company, year(`date`) as `years`, sum(total_laid_off) as total_laid_off
from layoffs_staging2
group by company, year(`date`)
)
select *, dense_rank() over (partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null 
order by 4 asc;


