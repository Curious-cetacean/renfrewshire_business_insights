SELECT BusinessName, PostCode, RatingValue 
FROM establishments;

SELECT DISTINCT RatingValue
FROM establishments;

--Find the business with the Improvement Required rating
SELECT BusinessName, PostCode, RatingDate, RatingValue
FROM establishments
WHERE RatingValue = 'Improvement Required'
ORDER BY RatingDate DESC;

--Count number of businesses in each hygiene rating
SELECT COUNT(*), RatingValue
FROM establishments
GROUP BY RatingValue
ORDER BY COUNT(*) DESC;

--Count businesses by area post code (PA1, PA2, ...)
SELECT COUNT(*) as "Number of businesses", SUBSTRING(PostCode, 1, instr(PostCode, ' ')) as PartPostCode
FROM establishments
GROUP BY PartPostCode
ORDER BY "Number of businesses" DESC;

--List all the available Business Type IDs
SELECT DISTINCT BusinessType, BusinessTypeID
FROM establishments;

--Count number of businesses in each Business Type ID
SELECT COUNT(*) as "Number of businesses", BusinessType, BusinessTypeID
FROM establishments
GROUP BY BusinessTypeID
ORDER BY "Number of Businesses" DESC;

