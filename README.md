# SQLTasks
Some SQL tasks to retrieve and add specific information to and from a database.


Step 0:
download and unzip the dataset from herE: http://files.grouplens.org/datasets/movielens/ml-latest.zip
a description of the datasets is provided here: http://files.grouplens.org/datasets/movielens/ml-latest-small-README.html

Step 1. Load files MOVIES and RATINGS into its own DataFrame (DF) using the same names as the original filenames.

To show your work: Print the schema for each dataframe. The dataframes should have the following structure:

ratings(userId,movieId,rating,timestamp)
movies(movieId,title,genres)

Step 2 From the Movies DataFrame: for each Row extract its genres from the genres column, split the genres into a list [genre_1, genre_2,...], and save all pairs <movieId, genre_1>, <movieId, genre_2>... into a CSV file called movieGenres.csv.
Note that one Movie will have more than one record, one for each Genre associated with the Movie.

Note: if you cannot write to file (there are known issues on some Windows configurations), please use this .csv file. But make sure you document your file writing code so the demonstrators can check that step 2 was done correctly.
 
Step 3: Load the movieGenres.csv file into a new DataFrame called movieGenres. Show that it has the expected schema:
   movieGenres(movieId, genre)
Show the top 50 row of this dataframe, ordered by movieId (descending) DataFrame, i.e., when a movie has multiple genres)

Step 4 Using the movieGenres  DF you produced in (3), create a new DF:
genrePopularity(genre, moviesCount) 
containing all genres along with the numbers of movies that are associated to that genre (popularity), in decreasing order of popularity.
Report the top 10 rows ordered by count (descending), that is, the top 10 most popular genres.

Step 5 Consider the top 10 genres identified in (4). For each genre G in this list, find the user U who has rated the highest number of movies that have genre G.
Report a list of pairs <G, U>

Step 6 For each userId in the ratings DataFrame, compute the number of movies that have been rated by that user (ratingsCount).
Consider the top 10 users by ratingsCount.  For those users U, find the most common genre among the movies they have rated. 
Report a list of tuples of the form  <userId, ratingsCount, mostCommonGenre> for these 10 users.

Step 7. For each movie, compute its average rating and the variance.
Report a list with the top 10 movies by average rating, along with their average rating and the variance.
