
USE olympic_dataset;

CREATE TABLE IF NOT EXISTS athlete_event_results (
    edition VARCHAR(255),
    edition_id INT,
    country_noc VARCHAR(10),
    sport VARCHAR(255),
    event VARCHAR(255),
    result_id INT,
    athlete VARCHAR(255),
    athlete_id INT,
    pos VARCHAR(50),
    medal VARCHAR(20),
    isTeamSport VARCHAR(10)
);

LOAD DATA INFILE '/import/athlete_event_results.csv'
INTO TABLE athlete_event_results
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(edition, edition_id, country_noc, sport, event,
 result_id, athlete, athlete_id, pos, @medal, isTeamSport)
SET medal = NULLIF(@medal, '');
