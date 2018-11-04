package tryspark;

import static org.apache.spark.sql.functions.callUDF;
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
//+------------------+----+---------+-------------------+
//|              sump| cnt|geonameId|        countryName|
//+------------------+----+---------+-------------------+
//| 559990.8999633789|1120|  6252001|    "United States"|
//|126911.90008544922| 254|  1814991|              China|
//| 71033.39999389648| 142|  1861060|              Japan|
//| 46460.10009765625|  93|  2921044|            Germany|
//| 41081.20001220703|  82|  2635167|   "United Kingdom"|
//| 40283.20004272461|  81|  1835841|"Republic of Korea"|
//| 32557.20001220703|  65|  3017382|             France|
//|29975.200073242188|  60|  6251999|             Canada|
//|28451.199981689453|  57|  3469034|             Brazil|
//| 25028.09994506836|  50|  3175395|              Italy|
//+------------------+----+---------+-------------------+
import static org.apache.spark.sql.functions.col;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import schema.CountryIP;
import schema.CountryName;
import schema.Product;
import udf.UDFGetGeoID;

public class SparkDataframe {
    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static String MYSQL_DB = "dbo";
    private static String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost/";
    private static String MYSQL_USERNAME = "root";
    private static String MYSQL_PWD = "password";

    private static final String DATA_PATH = "/Users/Shared/test/";
    private static final String INP_PRODUCT = "input3000.txt";
    private static final String INP_COUNTRYIP = "CountryIP.csv";
    private static final String INP_COUNTRYNAME = "CountryName.csv";
    private static final String EXT = "csv";
    private static final String OUT_NAME_51 = "table51";
    private static final String OUT_NAME_52 = "table52";
    private static final String OUT_NAME_63 = "table63";

    private static String PRODUCT_PATH = DATA_PATH + INP_PRODUCT;
    private static String COUNTRYIP_PATH = DATA_PATH + INP_COUNTRYIP;
    private static String COUNTRYNAME_PATH = DATA_PATH + INP_COUNTRYNAME;

    private static final String OUT_51_PATH = OUT_NAME_51 + "." + EXT;
    private static final String OUT_52_PATH = OUT_NAME_52 + "." + EXT;
    private static final String OUT_63_PATH = OUT_NAME_63 + "." + EXT;

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

    /**
     * Register UDF
     * 
     * @param spark    spark session
     * @param filename country ip addresses filename
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static void setupUDFs(SparkSession spark, String filename) throws FileNotFoundException, IOException {
        UDFGetGeoID.init(filename, spark);
        spark.udf().registerJava("getGeoId", UDFGetGeoID.class.getName(), DataTypes.LongType);
    }

    /**
     * Register UDF
     * 
     * @param spark session
     * @param ar    array of CountryIP
     */
    public static void setupUDFs(SparkSession spark, CountryIP[] ar) {
        UDFGetGeoID.init(ar, spark);
        spark.udf().registerJava("getGeoId", UDFGetGeoID.class.getName(), DataTypes.LongType);
    }

    /**
     * Select top 10 most frequently purchased categories
     * 
     * @param df list of Products
     * @return dataset [category, count]
     */
    public static Dataset<Row> task_51(Dataset<Row> df) {
        return df.select("category").groupBy("category").count().withColumnRenamed("count", "cnt")
                .orderBy(col("cnt").desc()).limit(10);
    }

    /**
     * Select top 10 most frequently purchased product in each category
     * 
     * @param df list of Products
     * @return dataset [name of product, category, count]
     */
    public static Dataset<Row> task_52(Dataset<Row> df) {
        return df.select("name", "category").groupBy("name", "category").count()
                .withColumnRenamed("count", "cnt").as("b")
                .join((df.select("category").groupBy("category").count().orderBy(col("count").desc()).as("a")),
                        col("a.category").equalTo(col("b.category")), "inner")
                .orderBy(col("cnt").desc()).limit(10).select("name", "b.category", "cnt");
    }

    /**
     * 
     * Select top 10 countries with the highest money spending (fast)
     *
     * @param df        list of Product
     * @param dfGeoName list of CountryName
     * @return Dataset [sum, count, geoname id, country name]
     */
    public static Dataset<Row> task_63(Dataset<Row> df, Dataset<Row> dfGeoName) {
        return df.select(col("ip"), col("IPAsLong"), col("price"), callUDF("getGeoId", col("IPAsLong")))
                .withColumnRenamed("UDF(IPAsLong)", "getGeoId")
                .withColumn("price", df.col("price").cast("Float"))
                .join(dfGeoName.as("a"), col("a.geonameId").equalTo(col("getGeoId")), "inner")
                .groupBy(col("a.geonameId"), col("a.countryName"))
                .agg(functions.sum("price").as("sump"), functions.count("*").as("cnt"))
                .orderBy(col("sump").desc()).limit(10)
                .select(col("sump"), col("cnt"), col("a.geonameId"), col("countryName"));
    }

    /**
     * 
     * Select top 10 countries with the highest money spending (slow)
     * 
     * @param df        list of Products
     * @param dfGeoIP   list of CountryIP
     * @param dfGeoName list of CountryName
     * @return Dataset [sum, count, geoname id, country name]
     * 
     *         SQL equivalent:
     *
     *         SELECT SUM(t.price) as summ, count(*) as cnt, t.geonameId,
     *         tcn.countryName FROM (SELECT tp.price, tp.IP, tc.Network,
     *         tc.geonameId FROM (select price, IP, IPAsLong from product) tp,
     *         (select geonameId, Network, StartIPAsLong, EndIPAsLong from
     *         countryip) tc WHERE tp.IPAsLong <= tc.EndIPAsLong AND tp.IPAsLong >=
     *         tc.StartIPAsLong ORDER BY tc.geonameId) t INNER JOIN countryname tcn
     *         ON t.geonameId = tcn.geonameId GROUP BY t.geonameId, tcn.countryName
     *         ORDER BY summ DESC LIMIT 10");
     *
     */
    public static Dataset<Row> task_63a(Dataset<Row> df, Dataset<Row> dfGeoIP, Dataset<Row> dfGeoName) {
        return df.select("ip", "IPAsLong", "price").withColumn("price", df.col("price").cast("Float"))
                .join(dfGeoIP.as("b"),
                        (col("b.EndIPAsLong").$greater(df.col("IPAsLong")))
                                .and(col("b.StartIPAsLong").$less(df.col("IPAsLong"))),
                        "inner")
                .join(dfGeoName.as("a"), col("a.geonameId").equalTo(col("b.geonameId")), "inner")
                .groupBy(col("a.geonameId"), col("a.countryName"))
                .agg(functions.sum("price").as("sump"), functions.count("*").as("cnt"))
                .orderBy(col("sump").desc()).limit(10)
                .select(col("sump"), col("cnt"), col("a.geonameId"), col("countryName"));
    }

    public static void main(String args[])
            throws ClassNotFoundException, FileNotFoundException, IOException {

        if (args.length == 7) {
            PRODUCT_PATH = args[0];
            COUNTRYIP_PATH = args[1];
            COUNTRYNAME_PATH = args[2];
            MYSQL_CONNECTION_URL = args[3];
            MYSQL_DB = args[4];
            MYSQL_USERNAME = args[5];
            MYSQL_PWD = args[6];
        } else {
            System.out.println("Usage: app <product_path> <country_ip_path> <country_name_path>");
            System.out.println("           <mysql_url> <mysql_db> <mysql_user> <mysql_pwd>");
        }
        System.out.println();
        System.out.println(String.format("product: %s", PRODUCT_PATH));
        System.out.println(String.format("countryIP: %s", COUNTRYIP_PATH));
        System.out.println(String.format("CountryName: %s", COUNTRYNAME_PATH));
        System.out.println(String.format("MySql url: %s", MYSQL_CONNECTION_URL));
        System.out.println(String.format("MySql database: %s", MYSQL_DB));
        System.out.println(String.format("MySql user: %s", MYSQL_USERNAME));
        System.out.println(String.format("MySql pwd: %s", MYSQL_PWD));
        System.out.println();

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", MYSQL_USERNAME);
        connectionProperties.put("password", MYSQL_PWD);

        try {
            prepareMySql(MYSQL_DB);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        //
        // become a record in RDD
        //

        // Define Spark Configuration
        SparkConf conf = new SparkConf().setAppName("Getting-Started").setMaster("local[*]");

        // Create Spark Context with configuration
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        // Create Dataframe
        Dataset<Row> df = spark.createDataFrame(
                sc.textFile(PRODUCT_PATH).map(f -> new Product(f.split(","))),
                Product.class);

        System.out.println("Table product");
        df.select(col("*")).limit(3).show();

        long startTime = 0;

        //
        // 5.1
        //
        System.out.println("Select top 10  most frequently purchased categories:");
        startTime = System.currentTimeMillis();
        Dataset<Row> df_51 = task_51(df);
        df_51.show();
        System.out.println(System.currentTimeMillis() - startTime);
        try {
            df_51.select("category", "cnt").write().mode(SaveMode.Overwrite).csv(OUT_51_PATH);
            df_51.write().mode(SaveMode.Overwrite).jdbc(MYSQL_CONNECTION_URL + MYSQL_DB, OUT_NAME_51,
                    connectionProperties);
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }

        //
        // 5.2
        //
        System.out.println("Select top 10 most frequently purchased product in each category:");
        startTime = System.currentTimeMillis();
        Dataset<Row> df_52 = task_52(df);
        df_52.show();
        System.out.println(System.currentTimeMillis() - startTime);
        try {
            df_52.select("name", "category", "cnt").write().mode(SaveMode.Overwrite).csv(OUT_52_PATH);
            df_52.write().mode(SaveMode.Overwrite).jdbc(MYSQL_CONNECTION_URL + MYSQL_DB, OUT_NAME_52,
                    connectionProperties);
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }

        //
        // 6.3 with country name (fast)
        //
        System.out.println("Select top 10 countries with the highest money spending (fast)");

        setupUDFs(spark, COUNTRYIP_PATH);

        Dataset<Row> dfGeoName = spark.createDataFrame(
                sc.textFile(COUNTRYNAME_PATH).map(f -> new CountryName(f.split(","))),
                CountryName.class).cache();

        startTime = System.currentTimeMillis();
        Dataset<Row> df_63 = task_63(df, dfGeoName);
        df_63.cache();
        df_63.show();
        System.out.println(System.currentTimeMillis() - startTime);
        try {
            df_63.select("sump", "cnt", "geonameId", "countryName").write().mode(SaveMode.Overwrite).csv(OUT_63_PATH);
            df_63.write().mode(SaveMode.Overwrite).jdbc(MYSQL_CONNECTION_URL + MYSQL_DB, OUT_NAME_63,
                    connectionProperties);
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }

        //
        // 6.3 with country name
        //
        System.out.println("Select top 10 countries with the highest money spending");
        Dataset<Row> dfGeoIP = spark.createDataFrame(
                sc.textFile(COUNTRYIP_PATH).map(f -> new CountryIP(f.split(","))),
                CountryIP.class).cache();

        startTime = System.currentTimeMillis();
        Dataset<Row> df_63a = task_63a(df, dfGeoIP, dfGeoName);
        df_63a.cache();
        df_63a.show();
        System.out.println(System.currentTimeMillis() - startTime);

        sc.close();
    }
}
