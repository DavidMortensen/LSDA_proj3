package dk.itu.LSDA.projects.project3

import com.typesafe.config.ConfigFactory
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.linalg._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._


object SentimentAnalyser {

  //create a spark session
  val spark = SparkSession
    .builder()
    .appName("AmazonReviewsSentimentAnalyser")
    .master("local[4]")
    .getOrCreate()


  import spark.implicits._


  /**
    * The amazon review dataset does not have a unique id for each row, therefore, you need to generate a new column called "id"
    * that represents a unique id for each row in the data
    * to do that, check the function: monotonically_increasing_id described in: https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/functions.html
    *
    * You will also need to concatenate the two columns "reviewText" and "summary" in the data to one column "text"
    * to do that, check the function: concat described in: https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/functions.html
    *
    * @param inputPath : the path to the amazon reviews dataset
    * @return a DataFrame that has the following columns: id, overall, text
    */
  def loadAmazonReviews(inputPath : String): DataFrame = {
    val dataDF = spark
      .read
      .json(inputPath)
      .withColumn("id", monotonically_increasing_id())
      .withColumn("text", concat(col("reviewText"), lit(" "), (col("summary"))))
      return dataDF.select("id", "overall", "text")
  }

  /**
    *
    * @param inputPath: the path to the glove dataset
    * @return a DataFrame that has the following columns: word, vec
    */
  def loadGloVe(inputPath : String):DataFrame={
    spark
      .read
      .text(inputPath)
      .map(t => t.mkString.split(" "))
      .map  (r => (r.head, r.tail.toList.map (_.toDouble)))
      .withColumnRenamed ("_1", "word" )
      .withColumnRenamed ("_2", "vec")
  }




  /**
    * Given a list of vectors (embeddings), each of Double values, calculate the sum of these vectors and then divide it by their number
    * to get the average
    *
    * NOTE 1: Seq[T] + Seq[T] is not defined, therefore, you will need to use the sumVect above to calculate that.
    * NOTE 2: You will probably need foldLeft to iterate over all the vectors and add their values
    */
  val averageVectors = udf{
    (embeddings : Seq[Seq[Double]]) =>
      {
        /**
          * helping function referenced by the udf averageVectors
          * note that if one of the vectors is empty, you need to return the other vectors as the sum of both
          * @param v1
          * @param v2
          * @return the sum of vectors v1 and v2
          */
        def sumVect(v1:Seq[Double], v2:Seq[Double]):Seq[Double] = ???

        //return value for the udf averageVectors
        ???
      }
  }


  /**
    * This function takes as input two DataFrames: reviews and glove and returns a DataFrame representing the reviews
    * after converting each review text into an Embedding (from GloVe corpus) represented as a vector of double values
    * To generate the o/p dataframe you need to follow these steps:
    * 1 - tokenize the input text in the review dataframe using org.apache.spark.ml.feature.Tokenizer
    *     described in https://spark.apache.org/docs/latest/ml-features.html#tokenizer
    *
    * 2 - For each word in the tokenized review text, replace it with the corresponding vector from the glove dataframe.
    * Important note: A computation on one DataFrame (for example in a map transformation)  cannot reference another Dataframe.
    * You cannot access other DataFrame in the closure of a function value passed to a HOF
    * operating on a DataFrame. This means that we cannot use map to translate reviews to embedding
    * vectors. This would require accessing the GLoVe DataFrame in the closure of the map’s argument.
    * You get an inexplicable exception when you attempt that.
    *
    * Objective of a possible solution for step 2: Make sure that the data and the computation are in a suitable format, which Spark can distribute.
    *
    * Proposed  solution: Flatten the reviews DataFrame rows which contain a list of words by creating a separate entry(row) for each word
    * (as is typically done when normalizing data to 1st normal form in databases). Once the data is in
    * this format, you can join the Dataset of GLoVe with the reviews over the word column (as you
    * would do it, if you implemented this in a relational database).
    *
    * NOTE: check the function explode described in: https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/functions.html
    *
    * 3 - Finally, for each row in the original  review DataFrame, take the average for all of the embeddings (vectors)
    *     created in the previous step (each vector represents an embedding of a word in the review text). To do this, you will need to
    *     group the rows that were flattened in the previous step and that are related to the same review and then calculate the average
    *     of thse embeddings (vectors)
    *
    * NOTE: You will need to use the user defined function (udf): averageVectors to calculate the average of vectors
    * You can find the examples at these links useful:
    *     countTokens UDF at: https://spark.apache.org/docs/latest/ml-features.html#tokenizer
    *     groupBy followed by agg at: https://docs.databricks.com/spark/latest/spark-sql/udaf-scala.html
    * Hint: You will need to check the function: collect_list applied to an aggregated column which you will need to group the embeddings
    * to one column of type Seq[ Seq[Double] ]
    *
    * @param reviews
    * @param glove
    * @return a DataFrame that has the following columns: id, overall, averageVectorEmbedding
    */
  def generateAvgWordEmbeddingVector(reviews : DataFrame, glove: DataFrame) : DataFrame = ???


  /**
    * Convert the values of "overall" that are in the range of 1 to 5 into one of three values 0, 1, or 2.
    * We will only consider three classes: negative(0) for overall
    * rating of 1 or 2, neutral(1) for rating 3, and positive (2) for rating 4 or 5. You will need to remap the ’overall’
    * rating column in the data from five classes to three classes before training.
    *
    * Summary: {1,2} = 0, {3} = 1, {4,5} = 2
    *
    * @param reviews
    * @return a DataFrame that has the following columns: id, normalizedRating, averageVectorEmbedding
    */
  def mapReviewRatings(reviews : DataFrame) : DataFrame = {
    val normalizedRating = udf {(rating : Double) => ??? }
    ???
  }

  /**
    * Convert the review average embedding represented as an Array[Double] to a DenseVector
    */
  val createDenseVector = udf{(vector: Seq[Double]) => new DenseVector(vector.toArray)}

  /**
    * convert the input that has the following columns: id, overall, averageVectorEmbedding (which is a vector of double values) into
    * a DataFrame that the classifier can use. This would require the following steps:
    * 1 - map the ratings of all reviews into 3 classes using the helping function: mapReviewRatings
    * 2 - Convert the review average embedding represented as an Array[Double] to a DenseVector by calling createDenseVector for
    *     this column
    * 3 - Rename the columns of the DataFrame as follows: normalizedRating => label and averageVectorEmbedding => features
    * Make sure that you use "drop" to remove any unnecessary columns.
    *
    * @param data
    * @return a DataFrame that has the following columns: id, label, features
    */
  def prepareDatasetForClassifier(data: DataFrame):DataFrame = ???

  /**
    * You need to follow the example described at this
    * link: https://spark.apache.org/docs/latest/ml-classification-regression.html#multilayer-perceptron-classifier
    * to create a classifier, split the input dataset into a training and testing sets, train the classifier and then validate it
    *
    * @param reviews
    */
  def trainAndValidateModel(reviews: DataFrame, numFeatures: Int, hiddenLayers1 : Int, hiddenLayers2 : Int, numIterations: Int):Unit = ???


  def main(args: Array[String]): Unit = {

    //load configuration file
    val conf = if(args.length >= 1) ConfigFactory.load(args(0)) else ConfigFactory.load()

    //load amazon reviews dataset
    val amazonDataFileName = conf.getString("Project3.amazonReviewFilePath")
    val amazonReviewsDF = loadAmazonReviews(amazonDataFileName)
    amazonReviewsDF.show()

    //load glove corpus
    //val gloveDataFileName = conf.getString("Project3.gloveFilePath")
    //val gloveDF = loadGloVe(gloveDataFileName)

    //Phase 1: generate an embedding (average vector for all the words in the review) for each tuple/row in the input dataset
    //val amazonReviewsAvgVectorDF = generateAvgWordEmbeddingVector(amazonReviewsDF,gloveDF)

    //Phase 2 and 3: prepare the data for the classifier, then train it and cross-validate
    //val preparedDataDF = prepareDatasetForClassifier(amazonReviewsAvgVectorDF)


    //val numFeatures = conf.getInt("Project3.numFeatures")
    //val hiddenLayers1 = conf.getInt("Project3.hiddenLayers1")
    //val hiddenLayers2 = conf.getInt("Project3.hiddenLayers2")
    //val numIterations = conf.getInt("Project3.numIterations")
    //trainAndValidateModel(preparedDataDF, numFeatures,hiddenLayers1, hiddenLayers2, numIterations)


    //close spark session
    spark.close()
  }

}
