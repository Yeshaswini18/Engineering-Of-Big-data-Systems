--group ratings with respective movieId

ratings = LOAD '/Users/sankeerthvs/NEU/sem4/engineering_big-data_systems/datasets/project_datasets/ratings_small.csv' USING PigStorage(',');
selected = FOREACH ratings GENERATE $2 as rating, $1 as movieId;
filteredData = FILTER selected BY rating < 3;
orderedData = ORDER filteredData BY movieId ASC;
groupData = GROUP filteredData BY movieId;
DUMP groupData

--join ratings and movie datasets to view the title and rating together

movies = LOAD '/Users/sankeerthvs/NEU/sem4/engineering_big-data_systems/datasets/project_datasets/movies_metadata.csv' USING PigStorage('|');
selected_movies = FOREACH movies GENERATE $5 as movieId, $8 as movieName;
inner_join = JOIN orderedData by movieId, selected_movies by movieId;
DUMP inner_join

--answer stored in pig 
--store inner_join INTO 'pig_answer.txt' using PigStorage(',');

