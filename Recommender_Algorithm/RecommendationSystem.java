package edu.nyu.bigdata.mllib;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;

import scala.Tuple2;

public class RecommendationSystem {

	public static void main(String[] args) {

		// Create Java spark context
		SparkConf conf = new SparkConf()
				.setAppName("Collaborative Filtering Document rating");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Read user-document rating file. format - userId,itemId,rating
		JavaRDD<String> userDocumentRatingsFile = sc
				.textFile("hdfs://babar.es.its.nyu.edu:8020/" + args[0]);

		// Map file to Ratings(user,document,rating) tuples
		JavaRDD<Rating> ratings = userDocumentRatingsFile
				.map(new Function<String, Rating>() {
					private static final long serialVersionUID = 1L;

					public Rating call(String s) {
						String[] sarray = s.split(",");
						return new Rating(sarray[0].hashCode(), Integer
								.parseInt(sarray[1]), Double
								.parseDouble(sarray[2]));
					}
				});

		// Build the recommendation model using ALS

		int rank = 10; // 10 latent factors
		int numIterations = 10; // number of iterations

		MatrixFactorizationModel model = ALS.trainImplicit(
				JavaRDD.toRDD(ratings), rank, numIterations);

		// Get top 3 recommendations for every user and scale ratings from 0 to
		// 1
		JavaRDD<Tuple2<Object, Rating[]>> userRecs = model
				.recommendProductsForUsers(3).toJavaRDD();
		JavaRDD<Tuple2<Object, Rating[]>> userRecsScaled = userRecs
				.map(new Function<Tuple2<Object, Rating[]>, Tuple2<Object, Rating[]>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Object, Rating[]> call(
							Tuple2<Object, Rating[]> t) {
						Rating[] scaledRatings = new Rating[t._2().length];
						for (int i = 0; i < scaledRatings.length; i++) {
							double newRating = Math.max(
									Math.min(t._2()[i].rating(), 1.0), 0.0);
							scaledRatings[i] = new Rating(t._2()[i].user(), t
									._2()[i].product(), newRating);
						}
						return new Tuple2<>(t._1(), scaledRatings);
					}
				});
		JavaPairRDD<Object, Rating[]> userRecommended = JavaPairRDD
				.fromJavaRDD(userRecsScaled);

		// Map ratings to 1 or 0, 1 indicating a movie that should be
		// recommended
		JavaRDD<Rating> binarizedRatings = ratings
				.map(new Function<Rating, Rating>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Rating call(Rating r) {
						double binaryRating;
						if (r.rating() > 0.0) {
							binaryRating = 1.0;
						} else {
							binaryRating = 0.0;
						}
						return new Rating(r.user(), r.product(), binaryRating);
					}
				});

		// Group ratings by common user
		JavaPairRDD<Object, Iterable<Rating>> userDocuments = binarizedRatings
				.groupBy(new Function<Rating, Object>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Object call(Rating r) {
						return r.user();
					}
				});

		// Get true relevant documents from all user ratings
		JavaPairRDD<Object, List<Integer>> userDocList = userDocuments
				.mapValues(new Function<Iterable<Rating>, List<Integer>>() {
					/**
				 * 
				 */
					private static final long serialVersionUID = 1L;

					@Override
					public List<Integer> call(Iterable<Rating> docs) {
						List<Integer> products = new ArrayList<>();
						for (Rating r : docs) {
							if (r.rating() > 0.0) {
								products.add(r.product());
							}
						}
						return products;
					}
				});

		JavaPairRDD<Object, List<Integer>> userRecommendedList = userRecommended
				.mapValues(new Function<Rating[], List<Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public List<Integer> call(Rating[] docs) {
						List<Integer> products = new ArrayList<>();
						for (Rating r : docs) {
							products.add(r.product());
						}
						return products;
					}
				});
		/**JavaRDD<Tuple2<List<Integer>, List<Integer>>> relevantDocs = userDocList
				.join(userRecommendedList).values();*/

		// model.save(sc.sc(), "/user/as9836/myCollaborativeFilter");
		//ratesAndPreds
		//		.saveAsObjectFile("hdfs://babar.es.its.nyu.edu:8020/user/as9836/recommendations-1");
		
		userRecommendedList.saveAsHadoopFile("recommendations-1", Text.class, List.class, SequenceFileOutputFormat.class);
		sc.close();
	}

}
