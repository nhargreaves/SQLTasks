package sparkPackage;

import static org.apache.spark.sql.functions.*;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.avg;
import java.util.List;
import java.util.ArrayList;
import java.io.*;

public class SparkSQL_Movies {

	private static String PATH = "src/main/resources/";

	private static String MOVIES_FILE = "movies.csv";
	private static String RATINGS = "ratings_small.csv";
	//use this instead for the main ratings file, but load times are very long
	//private static String RATINGS = "ratings.csv";
	private static String MOVIE_GENRES = "movieGenres.csv";

	static SparkConf conf = new SparkConf().setMaster("local").setAppName("My App");
	static JavaSparkContext sc = new JavaSparkContext(conf);
	static SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example")
			.config("spark.some.config.option", "some-value").getOrCreate();

	public static void main(String[] args) throws IOException {

		LogManager.getLogger("org").setLevel(Level.ERROR);

		/**
		 * 1. Load movies and ratings into their own dataframes. Print the schemas.
		 */
		System.out.println("Task 1: Load movies and ratings into their own dataframes.");
		Dataset<Row> movies = LoadMoviesMetadata().select("movieId", "title", "genres");
		System.out.println("Movies schema:");
		movies.printSchema();
		System.out.println("Ratings schema:");
		Dataset<Row> ratings = LoadRatingsMetadata().select("userId", "movieId", "rating", "timestamp");
		ratings.printSchema();
		

		/**
		 * 2. Extract genres from the Movie dataframe and save movie:genres in a list to
		 * a .csv file.
		 **/
		System.out.println("Task 2: Extract genres from the movie dataframe and save these in a list to a .csv file");

	    File f = new File("movieGenres.csv");
		FileWriter fw = new FileWriter("movieGenres.csv", true);
		String newLine = System.getProperty( "line.separator" );
		
		for (Row r : movies.collectAsList()) {
			int movieId = r.getAs("movieId");
			String genres = r.getAs("genres");
			String genresAr[] = genres.split("\\|");

			for (String genre : genresAr) {
				fw.append(movieId + "," + genre + newLine);
			}
		}
		
		fw.close();

		/**
		 * 3. Load the .csv file into a dataframe and print its schema. Show the first
		 * 50 entries, ordered by id descending.
		 */
		System.out.println("Task 3: Load the .csv file into a dataframe and print its schema");

		Dataset<Row> movieGenres = LoadGenresMetadata();
		System.out.println("Genres schema:");
		movieGenres.printSchema();

		movieGenres.orderBy(col("movieId").desc()).show(50);

		/**
		 * 4. Using the movieGenres DF, Create a dataframe genrePopularity containing
		 * all genres along with the number of movies associated with those genres, in
		 * decreasing order of popularity. Report the top ten.
		 */
		System.out.println("Task 4: Using the movieGenres DF, create a dataframe according to specs. Report top ten");

		Dataset<Row> genrePopularity = movieGenres.groupBy(col("genre")).count().orderBy(col("count").desc());
		genrePopularity.show(10);

		/**
		 * 5. Consider the top ten genres in (4). For each genre, find the user who has
		 * rated the highest number of movies. Report a list of pairs <G,U>.
		 */

		System.out.println("Task 5: For the genres in (4), find the user who has rated the highest number of movies. Report a list of pairs.");

		// join ratings with movieGenres
		Dataset<Row> joinedDF = ratings.join(movieGenres, movieGenres.col("movieId").equalTo(ratings.col("movieId")));

		// a list to store the results. Must output in <G>,<U>.
		List<String> userGenres = new ArrayList<String>(); // contains genres
		List<String> mostRated = new ArrayList<String>(); // contains final results to output

		joinedDF.createOrReplaceTempView("joinedView");

		// get list of genres
		for (Row r : genrePopularity.collectAsList()) {
			String rowGen = r.getAs("genre");
			System.out.println(rowGen);
			userGenres.add(rowGen);
		}

		for (int i = 0; i < 10; i++) {
			// this creates a list of users who have rated the genre
			String sql1 = "SELECT userId FROM joinedView WHERE genre = '" + userGenres.get(i) + "'";
			Dataset<Row> sqlResult = (spark.sql(sql1));

			// this is a count of how many times each user has rated this genre
			Dataset<Row> resultsList = sqlResult.groupBy(col("userId")).count().orderBy(col("count").desc());

			// list containing user IDs from resultsList
			List<Integer> topUsers = new ArrayList<Integer>();
			for (Row r : resultsList.collectAsList()) {
				int userGenre = r.getAs("userId");
				topUsers.add(userGenre);
			}
			// the list is compiled
			mostRated.add("<" + userGenres.get(i) + "," + topUsers.get(0) + ">");
		}

		// print the list
		for (int i = 0; i < mostRated.size(); i++) {
			System.out.println(mostRated.get(i));
		}

		/**
		 * 6. For each id in 'Ratings' computer number of movies rated by user. Consider
		 * top 10. Find the most common genre among their rated movies. Report a list in
		 * the format <userID, ratingsCount, mostCommonGenre>
		 **/
		// takes users who have rated most films
		Dataset<Row> totalRatings = ratings.groupBy(col("userId")).count().orderBy(col("count").desc());

		List<String> genreResults = new ArrayList<String>(); // list which will contain results

		List<Integer> userIds = new ArrayList<Integer>(); // list containing top user ids
		List<Long> counts = new ArrayList<Long>(); // list containing total ratings of user ids

		// this populates the lists from the dataframe
		for (Row r : totalRatings.collectAsList()) {
			int userIdRatings = r.getAs("userId");
			long countRatings = r.getAs("count");
			userIds.add(userIdRatings);
			counts.add(countRatings);
		}

		// iterates through all top ten users
		for (int i = 0; i < 10; i++) {
			// this gets the genres which have been rated by this user
			String sql2 = "SELECT genre FROM joinedView WHERE userID = '" + userIds.get(i) + "'";
			Dataset<Row> sql2Result = spark.sql(sql2);

			// this orders those genres in descending order of how many the user has rated
			Dataset<Row> resultsList2 = sql2Result.groupBy(col("genre")).count().orderBy(col("count").desc());

			// populates a list with genres most rated by respective users
			List<String> topGenre = new ArrayList<String>();
			for (Row row2 : resultsList2.collectAsList()) {
				String topUserGenre = row2.getAs("genre");
				topGenre.add(topUserGenre);
			}
			genreResults.add("<" + userIds.get(i) + "," + counts.get(i) + "," + topGenre.get(i) + ">");
		}

		System.out.println("Task 6: Genres most rated by top rating users");
		for (int i = 0; i < 10; i++) { 
			System.out.println(genreResults.get(i));
		}

		/**
		 * 7. For each movie, calculate the average rating and the variance. Report a
		 * list of the top ten by average rating, along with their average rating and
		 * the variance.
		 */
		System.out.println("Task 7: Averages and variances of movies");
		Dataset<Row> movieRatings = ratings.groupBy(col("movieId")).agg(avg("rating"), var_samp("rating"))
				.orderBy(col("movieId").desc());
		movieRatings.show();

		spark.stop();
	}

	private static Dataset<Row> LoadMoviesMetadata() {
		return spark.read().option("inferSchema", true).option("header", true).option("multLine", true)
				.option("mode", "DROPMALFORMED").csv(PATH + MOVIES_FILE);
	}

	private static Dataset<Row> LoadRatingsMetadata() {
		return spark.read().option("inferSchema", true).option("header", true).option("multLine", true)
				.option("mode", "DROPMALFORMED").csv(PATH + RATINGS);
	}

	private static Dataset<Row> LoadGenresMetadata() {
		return spark.read().option("inferSchema", true).option("header", true).option("multLine", true)
				.option("mode", "DROPMALFORMED").csv(PATH + MOVIE_GENRES);
	}
}