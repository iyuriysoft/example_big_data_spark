package tryspark;

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
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import schema.CountryIP;
import schema.CountryName;
import schema.Product;
import udf.UDFGetEndIP;
import udf.UDFGetIP;
import udf.UDFGetStartIP;

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
//
//Select top 10 countries with the highest money spending
//
//+------------------+----+---------+-------------------+
//|              summ| cnt|geonameId|        countryName|
//+------------------+----+---------+-------------------+
//| 559990.8999999998|1120|  6252001|    "United States"|
//|          126911.9| 254|  1814991|              China|
//| 71033.40000000001| 142|  1861060|              Japan|
//| 46460.09999999999|  93|  2921044|            Germany|
//|41081.200000000004|  82|  2635167|   "United Kingdom"|
//|40283.200000000004|  81|  1835841|"Republic of Korea"|
//|32557.199999999997|  65|  3017382|             France|
//|29975.200000000008|  60|  6251999|             Canada|
//|           28451.2|  57|  3469034|             Brazil|
//|25028.099999999995|  50|  3175395|              Italy|
//+------------------+----+---------+-------------------+

public class SparkSQL {
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

    private static final String PRODUCT_PATH = DATA_PATH + INP_PRODUCT;
    private static final String COUNTRYIP_PATH = DATA_PATH + INP_COUNTRYIP;
    private static final String COUNTRYNAME_PATH = DATA_PATH + INP_COUNTRYNAME;

    private static final String OUT_51_PATH = DATA_PATH + OUT_NAME_51 + "." + EXT;
    private static final String OUT_52_PATH = DATA_PATH + OUT_NAME_52 + "." + EXT;
    private static final String OUT_63_PATH = DATA_PATH + OUT_NAME_63 + "." + EXT;

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

    @Deprecated
    public static void setupUDFs(SQLContext sqlContext) {
        sqlContext.udf().register("getIP", (String s) -> UDFGetIP.class, DataTypes.LongType);
        sqlContext.udf().register("getStartIP", (String s) -> UDFGetStartIP.class, DataTypes.LongType);
        sqlContext.udf().register("getEndIP", (String s) -> UDFGetEndIP.class, DataTypes.LongType);
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

        // Create RDD
        JavaRDD<Product> rddP = sc.textFile(PRODUCT_PATH).map(f -> new Product(f.split(",")));
        // Create Dataframe
        Dataset<Row> df = spark.createDataFrame(rddP, Product.class);

        System.out.println("Table product");
        // Register the DataFrame as a temporary view
        df.createOrReplaceTempView("product");
        Dataset<Row> df2 = spark.sql("SELECT * FROM product LIMIT 3");
        df2.show(5, false);

        //
        // 5.1
        //
        System.out.println("Select top 10  most frequently purchased categories:");
        Dataset<Row> df_51 = spark
                .sql("SELECT category, COUNT(*) as cnt FROM product " + "GROUP BY category ORDER BY cnt DESC LIMIT 10");
        df_51.show();
        df_51.select("category", "cnt").write().mode(SaveMode.Overwrite).csv(OUT_51_PATH);
        df_51.write().mode(SaveMode.Overwrite).jdbc(MYSQL_CONNECTION_URL + MYSQL_DB, OUT_NAME_51, connectionProperties);

        //
        // 5.2
        //
        System.out.println("Select top 10 most frequently purchased product in each category:");
        Dataset<Row> df_52 = spark.sql("SELECT tp.name, tp.category, count(*) as cnt FROM product tp INNER JOIN "
                + "(select category, count(*) as c from product group by category order by c desc) tcat "
                + "ON tp.category = tcat.category " + "GROUP BY tp.name, tp.category ORDER BY cnt DESC LIMIT 10");
        df_52.show();
        df_52.select("name", "category", "cnt").write().mode(SaveMode.Overwrite).csv(OUT_52_PATH);
        df_52.write().mode(SaveMode.Overwrite).jdbc(MYSQL_CONNECTION_URL + MYSQL_DB, OUT_NAME_52, connectionProperties);

//        //
//        // 6.3 with ip
//        //
//        System.out.println("Select top 10 IP with the highest money spending:");
//        Dataset<Row> df_63i = spark
//                .sql("SELECT t.ip, sum(t.price) sump FROM product t GROUP BY t.ip ORDER BY sump DESC LIMIT 10");
//        df_63i.show();
//        df_63i.select("ip", "sump").write().mode(SaveMode.Overwrite).csv(OUT_63IP_PATH);
//        df_63i.write().mode(SaveMode.Overwrite).jdbc(MYSQL_CONNECTION_URL + MYSQL_DB, OUT_NAME_63IP,
//                connectionProperties);

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

        Dataset<Row> df_63 = spark.sql(
                "SELECT SUM(t.price) as summ, count(*) as cnt, t.geonameId, tcn.countryName "
                + "FROM (SELECT tp.price, tp.IP, tc.Network, tc.geonameId "
                      + "FROM (select price, IP, IPAsLong from product) tp, "
                      + "(select geonameId, Network, StartIPAsLong, EndIPAsLong from countryip) tc " 
                      + "WHERE tp.IPAsLong <= tc.EndIPAsLong AND tp.IPAsLong >= tc.StartIPAsLong ORDER BY tc.geonameId) t "
                + "INNER JOIN countryname tcn ON t.geonameId = tcn.geonameId "
                + "GROUP BY t.geonameId, tcn.countryName ORDER BY summ DESC LIMIT 10");

//        Dataset<Row> df_63a = spark.sql(
//                "SELECT tp.price, tp.IP, tc.Network, tc.geonameId FROM "
//                        + "(select price, IP, IPAsLong from product) tp, "
//                        + "(select geonameId, Network, StartIPAsLong, EndIPAsLong from countryip) tc "
//                        + "WHERE tp.IPAsLong <= tc.EndIPAsLong AND tp.IPAsLong >= tc.StartIPAsLong ORDER BY tc.geonameId");
//        df_63a.createOrReplaceTempView("product2");
//        df_63a.show();
//
//        System.out.println("Select top 10 2");
//        Dataset<Row> df_63b = spark.sql(
//                "SELECT SUM(t.price) as summ, count(*) as cnt, t.geonameId, tcn.countryName FROM product2 t "
//                        + "INNER JOIN countryname tcn ON t.geonameId = tcn.geonameId "
//                        + "GROUP BY t.geonameId, tcn.countryName ORDER BY summ DESC LIMIT 10");
        df_63.show();
        df_63.select("summ", "cnt", "geonameId", "countryName").write().mode(SaveMode.Overwrite).csv(OUT_63_PATH);
        df_63.write().mode(SaveMode.Overwrite).jdbc(MYSQL_CONNECTION_URL + MYSQL_DB, OUT_NAME_63, connectionProperties);

        sc.close();
    }
}
