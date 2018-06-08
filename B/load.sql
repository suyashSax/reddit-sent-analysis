LOAD DATA LOCAL INFILE '~/data/actor1.del' INTO TABLE Actor 
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INFILE '~/data/actor2.del' INTO TABLE Actor 
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INFILE '~/data/actor3.del' INTO TABLE Actor 
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INFILE '~/data/director.del' INTO TABLE Director 
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INFILE '~/data/movieactor1.del' INTO TABLE MovieActor
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INFILE '~/data/movieactor2.del' INTO TABLE MovieActor
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INFILE '~/data/movie.del' INTO TABLE Movie
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INFILE '~/data/moviedirector.del' INTO TABLE MovieDirector
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INFILE '~/data/moviegenre.del' INTO TABLE MovieGenre
FIELDS TERMINATED BY ',';

-- need to load tables: Review, MaxMovieID, MaxPersonID for 1C