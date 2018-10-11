package tryspark;

import static org.apache.spark.sql.functions.col;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import schema.CountryIP;
import schema.CountryName;
import schema.Product;

//|  category|cnt|
//+----------+---+
//| category9|163|
//| category2|162|
//| category5|162|
//| category3|162|
//|category12|159|
//|category10|156|
//|category18|155|
//|category16|154|
//| category8|151|
//| category7|150|
//+----------+---+
//
//Select top 10 most frequently purchased product in each category:
//
//
//[Stage 15:==========================================>           (158 + 2) / 200]
//+---------+----------+---+
//|     name|  category|cnt|
//+---------+----------+---+
//|product16| category3| 16|
//|product16| category0| 16|
//|product10| category3| 15|
//| product9| category9| 15|
//|product19|category12| 14|
//|product18|category18| 14|
//|product16|category16| 14|
//|product10|category18| 14|
//|product12| category5| 14|
//| product1|category10| 14|
//| product4| category3| 14|
//| product6| category8| 13|
//|product10| category2| 13|
//| product5|category14| 13|
//|product19| category7| 13|
//|product15| category1| 13|
//|product12| category9| 13|
//|product10| category7| 13|
//| product0|category12| 13|
//|product14| category5| 12|
//+---------+----------+---+
//only showing top 20 rows
//
//
//
//[Stage 22:===============================================>      (175 + 2) / 200]
//Select top 10 IP with the highest money spending:
//+--------------+-----+
//|            ip| sump|
//+--------------+-----+
//|   249.5.128.6|534.0|
//| 215.39.139.91|533.5|
//|249.70.127.130|532.1|
//|38.101.171.224|531.5|
//| 27.238.136.11|531.4|
//|  84.38.11.162|531.1|
//|116.48.124.164|530.8|
//| 20.25.239.179|530.3|
//| 62.158.125.95|528.6|
//| 176.154.86.16|528.3|
//+--------------+-----+
//
//Select top 10 countries with the highest money spending
//
//
//[Stage 43:>                 (0 + 2) / 2][Stage 44:>                 (0 + 0) / 2]
//[Stage 43:=========>        (1 + 1) / 2][Stage 44:>                 (0 + 1) / 2]
//+-----+--------------+---------+-------------------+---------------+
//| sump|            IP|geonameId|        countryName|        Network|
//+-----+--------------+---------+-------------------+---------------+
//|533.5| 215.39.139.91|  6252001|    "United States"|  215.32.0.0/11|
//|531.5|38.101.171.224|  6252001|    "United States"|38.101.128.0/18|
//|531.4| 27.238.136.11|  1835841|"Republic of Korea"|  27.232.0.0/13|
//|531.1|  84.38.11.162|  3144096|             Norway|   84.38.8.0/21|
//|530.8|116.48.124.164|  1819730|        "Hong Kong"|  116.48.0.0/15|
//|530.3| 20.25.239.179|  6252001|    "United States"|    20.0.0.0/11|
//|528.6| 62.158.125.95|  2921044|            Germany|  62.158.0.0/16|
//|528.3| 176.154.86.16|  3017382|             France| 176.128.0.0/10|
//+-----+--------------+---------+-------------------+---------------+

public class SparkDataframe {
    private static final String MYSQL_DB = "dbo";
    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost/";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PWD = "password";

    private static final String DATA_PATH = "/Users/Shared/test/";
    private static final String INP_PRODUCT = "input3000.txt";
    private static final String INP_COUNTRYIP = "CountryIP.csv";
    private static final String INP_COUNTRYNAME = "CountryName.csv";
    private static final String EXT = "csv";
    private static final String OUT_NAME_51 = "table51";
    private static final String OUT_NAME_52 = "table52";
    private static final String OUT_NAME_63 = "table63";
    private static final String OUT_NAME_63IP = "table63ip";   
    
    private static final String PRODUCT_PATH = DATA_PATH + INP_PRODUCT;
    private static final String COUNTRYIP_PATH = DATA_PATH + INP_COUNTRYIP;
    private static final String COUNTRYNAME_PATH = DATA_PATH + INP_COUNTRYNAME;

    private static final String OUT_51_PATH = DATA_PATH + OUT_NAME_51 + "." + EXT;
    private static final String OUT_52_PATH = DATA_PATH + OUT_NAME_52 + "." + EXT;
    private static final String OUT_63_PATH = DATA_PATH + OUT_NAME_63 + "." + EXT;
    private static final String OUT_63IP_PATH = DATA_PATH + OUT_NAME_63IP + "." + EXT;

    private static void prepareMySql(String dbname) throws ClassNotFoundException, SQLException {
        Class.forName(MYSQL_DRIVER);
        System.out.println("Connecting to database...");
        Connection conn = DriverManager.getConnection(MYSQL_CONNECTION_URL, MYSQL_USERNAME, MYSQL_PWD);
        System.out.println("Creating database...");
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbname);
        stmt.close();
        System.out.println("Database created successful");
    }

    public static void main(String args[]) throws ClassNotFoundException, SQLException {

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", MYSQL_USERNAME);
        connectionProperties.put("password", MYSQL_PWD);

        prepareMySql(MYSQL_DB);

        //
        // become a record in RDD
        //

        // Define Spark Configuration
        SparkConf conf = new SparkConf().setAppName("Getting-Started").setMaster("local[*]");

        // Create Spark Context with configuration
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Create RDD & Dataframe
        JavaRDD<Product> rddP = sc.textFile(PRODUCT_PATH).map(f -> new Product(f.split(",")));
        Dataset<Row> df = spark.createDataFrame(rddP, Product.class);

        System.out.println("Table product");
        df.createOrReplaceTempView("product");
        Dataset<Row> df2 = df.select("*").limit(3);
        df2.show(5, false);

        //
        // 5.1
        //
        System.out.println("Select top 10  most frequently purchased categories:");
        Dataset<Row> df_51 = df.select("category").groupBy("category").count().withColumnRenamed("count", "cnt")
                .orderBy(col("cnt").desc()).limit(10);
        df_51.show();
        df_51.printSchema();
        df_51.select("category", "cnt").write().mode(SaveMode.Overwrite).csv(OUT_51_PATH);
        df_51.write().mode(SaveMode.Overwrite).jdbc(MYSQL_CONNECTION_URL + MYSQL_DB, OUT_NAME_51, connectionProperties);

        //
        // 5.2
        //
        System.out.println("Select top 10 most frequently purchased product in each category:");
        Dataset<Row> df_52 = df.select("name", "category").groupBy("name", "category").count()
                .withColumnRenamed("count", "cnt").as("b")
                .join((df.select("category").groupBy("category").count().orderBy(col("count").desc()).as("a")),
                        col("a.category").equalTo(col("b.category")), "inner")
                .orderBy(col("cnt").desc()).limit(10).select("name", "b.category", "cnt");
        df_52.show();
        df_52.select("name", "category", "cnt").write().mode(SaveMode.Overwrite).csv(OUT_52_PATH);
        df_52.write().mode(SaveMode.Overwrite).jdbc(MYSQL_CONNECTION_URL + MYSQL_DB, OUT_NAME_52, connectionProperties);

        //
        // 6.3 with ip
        //
        System.out.println("Select top 10 IP with the highest money spending:");
        Dataset<Row> df_63i = df.select("ip", "price").withColumn("price", df.col("price").cast("Float")).groupBy("ip")
                .sum("price").withColumnRenamed("sum(price)", "sump").orderBy(col("sump").desc()).limit(10);
        df_63i.show();
        df_63i.select("ip", "sump").write().mode(SaveMode.Overwrite).csv(OUT_63IP_PATH);
        df_63i.write().mode(SaveMode.Overwrite).jdbc(MYSQL_CONNECTION_URL + MYSQL_DB, OUT_NAME_63IP,
                connectionProperties);

        //
        // 6.3 with country name
        //
        System.out.println("Select top 10 countries with the highest money spending");
        JavaRDD<CountryIP> rddGeoIP = sc.textFile(COUNTRYIP_PATH).map(f -> new CountryIP(f.split(",")));
        Dataset<Row> dfGeoIP = spark.createDataFrame(rddGeoIP, CountryIP.class);
        dfGeoIP.createOrReplaceTempView("countryip");

        JavaRDD<CountryName> rddGeoName = sc.textFile(COUNTRYNAME_PATH).map(f -> new CountryName(f.split(",")));
        Dataset<Row> dfGeoName = spark.createDataFrame(rddGeoName, CountryName.class);
        dfGeoName.createOrReplaceTempView("countryname");

        Dataset<Row> df_63 = df.select("ip", "IPAsLong", "price").withColumn("price", df.col("price").cast("Float"))
                .groupBy("ip", "IPAsLong").sum("price").withColumnRenamed("sum(price)", "sump")
                .orderBy(col("sump").desc()).limit(10)
                .join(dfGeoIP.as("b"),
                        (col("b.EndIPAsLong").$greater(df.col("IPAsLong")))
                                .and(col("b.StartIPAsLong").$less(df.col("IPAsLong"))),
                        "inner")
                .join(dfGeoName.as("a"), col("a.geonameId").equalTo(col("b.geonameId")), "inner")
                .orderBy(col("sump").desc()).limit(10)
                .select(col("sump"), col("IP"), col("a.geonameId"), col("countryName"), col("Network"));
        df_63.show();
        df_63.select("sump", "IP", "countryName").write().mode(SaveMode.Overwrite).csv(OUT_63_PATH);
        df_63.write().mode(SaveMode.Overwrite).jdbc(MYSQL_CONNECTION_URL + MYSQL_DB, OUT_NAME_63, connectionProperties);

        sc.close();
    }
}
