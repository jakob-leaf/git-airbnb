ALTER TABLE locations ADD COLUMN exchange FLOAT;

UPDATE locations
SET exchange = CASE country
    WHEN 'United States' THEN 1.0000
    WHEN 'The Netherlands' THEN 0.9506
    WHEN 'Belgium' THEN 0.9506
    WHEN 'Greece' THEN 0.9506
    WHEN 'Thailand' THEN 34.6341
    WHEN 'Spain' THEN 0.9506
    WHEN 'Australia' THEN 1.4011
    WHEN 'Belize' THEN 2.0000
    WHEN 'Germany' THEN 0.9506
    WHEN 'Italy' THEN 0.9506
    WHEN 'France' THEN 0.9506
    WHEN 'United Kingdom' THEN 0.7905
    WHEN 'Argentina' THEN 350.0000
    WHEN 'South Africa' THEN 18.1078
    WHEN 'Denmark' THEN 7.1400
    WHEN 'Ireland' THEN 0.9506
    WHEN 'Switzerland' THEN 0.8855
    WHEN 'China' THEN 7.2100
    WHEN 'Turkey' THEN 34.4653
    WHEN 'Portugal' THEN 0.9506
    WHEN 'Mexico' THEN 20.2313
    WHEN 'Canada' THEN 1.2651
    WHEN 'New Zealand' THEN 1.7031
    WHEN 'Norway' THEN 11.0716
    WHEN 'Czech Republic' THEN 22.1300
    WHEN 'Latvia' THEN 0.9506
    WHEN 'Brazil' THEN 5.2000
    WHEN 'Chile' THEN 900.0000
    WHEN 'Singapore' THEN 1.3445
    WHEN 'Sweden' THEN 11.0473
    WHEN 'Taiwan' THEN 32.5400
    WHEN 'Japan' THEN 155.5100
    WHEN 'Austria' THEN 0.9506
    ELSE NULL -- For countries not listed
END;

ALTER TABLE listings ADD COLUMN price_std FLOAT;

UPDATE listings as l
LEFT JOIN locations as e USING (location_id)
SET l.price_std = l.price / e.exchange;









