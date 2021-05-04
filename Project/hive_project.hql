CREATE TABLE IF NOT EXISTS RATINGS (userId Int, movieId Int, rating decimal(5,2), timestampR String) ROW FORMAT DELIMITED FIELDS TERMINATED BY "," LINES TERMINATED BY "\n";

set hive.cli.print.header=true;

LOAD DATA LOCAL INPATH '/Users/sankeerthvs/NEU/sem4/engineering_big-data_systems/datasets/project_datasets/ratings_small.csv' OVERWRITE INTO TABLE RATINGS;

CREATE TABLE IF NOT EXISTS MOVIES (adult String, collection String, budget bigint, genre String, homepage String, movieId bigint, imdbId String, language String, title String, overview String, popularity decimal(5,2), posterPath String, company String, country String, releaseDate date, revenue bigint, runtime double, spokenLanguage String, status String, tagline String, name String, video String, voteAverage double, voteCount bigint) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY "\n";

set hive.cli.print.header=true;

LOAD DATA LOCAL INPATH '/Users/sankeerthvs/NEU/sem4/engineering_big-data_systems/datasets/project_datasets/movies_metadata.csv' OVERWRITE INTO TABLE MOVIES;

Select m.title, r.rating from movies m join ratings r 