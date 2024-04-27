select * 
from layoffs

SELECT * INTO layoffs_staging FROM layoffs WHERE 1=0;

-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways


-- 1. Remove Duplicates
select *
from layoffs_staging

insert layoffs_staging
select * 
from layoffs

with cte as
(select * , ROW_NUMBER() over(partition by company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions order by company) as rownum
from layoffs_staging)
select *
from cte 
where rownum > 1

-- Confirming by looking for some company with duplicates
select *
from layoffs_staging 
where company = 'yahoo'     

-- We can't delete the duplicate rows as rownum is not a column in the table. It is just a CTE that we have created manually. 
---- one solution, which I think is a good one. Is to create a new table and add those row numbers in. Then delete the duplicates where row numbers are over 2

SELECT *
FROM layoffs_staging;

CREATE TABLE layoffs_staging3 (
    company VARCHAR(MAX),
    location VARCHAR(MAX),
    industry VARCHAR(MAX),
    total_laid_off INT,
    percentage_laid_off VARCHAR(MAX),
    date VARCHAR(MAX),
    stage VARCHAR(MAX),
    country VARCHAR(MAX),
    funds_raised_millions INT,
    row_num INT
);

INSERT INTO layoffs_staging3
(company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions, row_num)
SELECT 
    company,
    location,
    industry,
    total_laid_off,
    percentage_laid_off,
    date,
    stage,
    country,
    funds_raised_millions,
    ROW_NUMBER() OVER (
        PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions order by company
    ) AS row_num
FROM 
   layoffs_staging;

delete from layoffs_staging3
where row_num >= 2

-- just to confirm 
select * 
from layoffs_staging3
where row_num >= 2

--- 2. Standardizing the data and fixing errors


-- trimming the data using trim function so that all the spaces are even 
select company, trim(company)
from layoffs_staging3

update layoffs_staging3
set company = trim(company)

--- To ensure consistency, industries and countries with synonymous meanings are mapped to standardized names during data insertion
select distinct industry
from layoffs_staging3
order by industry


update layoffs_staging3
set industry = 'Crypto' 
where industry like 'Crypto%'

select distinct country 
from layoffs_staging3
order by 1

update layoffs_staging3
set country = 'United States'
where country = 'United States.'

select * from layoffs_staging3

--- Standardize the date format
select Date, CONVERT(date,date) as DateConverted
from layoffs_staging3

alter table layoffs_staging3
add DateConverted date;

Update layoffs_staging3
Set DateConverted = CONVERT(date,date) 

alter table layoffs_staging3
drop Column Date;

--- 3. working on NULL and blank values
select * 
from layoffs_staging3
where industry is null or industry = ' '

select *
from layoffs_staging3
where company = 'Airbnb'

select t1.industry , t2.industry
from layoffs_staging3  t1
join layoffs_staging3 t2
on t1.company = t2.company 
where t1.industry is null or t1.industry = ' ' 
and t2.industry is not null

UPDATE t1
SET t1.industry = t2.industry 
FROM layoffs_staging3 t1
INNER JOIN layoffs_staging3 t2 ON t1.company = t2.company 
WHERE (t1.industry IS NULL OR t1.industry = ' ') 
AND t2.industry IS NOT NULL;


---removing row_num column as we do not require it anymore
alter table layoffs_staging3
drop column row_num

select * 
from layoffs_staging3
