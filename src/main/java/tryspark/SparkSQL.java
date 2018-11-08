package tryspark;

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
import org.apache.spark.sql.types.DataTypes;

import schema.CountryIP;
import schema.CountryName;
import schema.Product;
import udf.UDFGetGeoID;;

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
     * @param spark
     * @param filename
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
     * @param spark
     * @param ar
     */
    public static void setupUDFs(SparkSession spark, CountryIP[] ar) {
        UDFGetGeoID.init(ar, spark);
        spark.udf().registerJava("getGeoId", UDFGetGeoID.class.getName(), DataTypes.LongType);
    }

    /**
     * Select top 10 most frequently purchased categories
     * 
     * @param spark SparkSession
     * @return dataset [category, count]
     */
    public static String task_51(String productTable) {
        return String.format("SELECT category, COUNT(*) as cnt FROM %s GROUP BY category ORDER BY cnt DESC LIMIT 10",
                productTable);
    }

    /**
     * Select top 10 most frequently purchased product in each category
     * 
     * @param productTable table name
     * @return sql query [name of product, category, count]
     */
    public static String task_52(String productTable) {
        return String.format("SELECT tp.name, tp.category, count(*) as cnt FROM %s tp INNER JOIN "
                + "(select category, count(*) as c from %s group by category order by c desc) tcat "
                + "ON tp.category = tcat.category " + "GROUP BY tp.name, tp.category ORDER BY cnt DESC LIMIT 10",
                productTable, productTable);
    }

    /**
     * 
     * Select top 10 countries with the highest money spending
     * 
     * @param productTable     table name
     * @param countryipTable   table name
     * @param countrynameTable table name
     * 
     * @return sql query [sum, count, geoname id, country name]
     */
    public static String task_63a(String productTable, String countryipTable, String countrynameTable) {
        return String.format(
                "SELECT SUM(t.price) as summ, count(*) as cnt, t.geonameId, tcn.countryName "
                        + "FROM (SELECT tp.price, tp.IP, tc.Network, tc.geonameId "
                        + "FROM (select price, IP, IPAsLong from %s) tp, "
                        + "(select geonameId, Network, StartIPAsLong, EndIPAsLong from %s) tc "
                        + "WHERE tp.IPAsLong <= tc.EndIPAsLong AND tp.IPAsLong >= tc.StartIPAsLong ORDER BY tc.geonameId) t "
                        + "INNER JOIN %s tcn ON t.geonameId = tcn.geonameId "
                        + "GROUP BY t.geonameId, tcn.countryName ORDER BY summ DESC LIMIT 10",
                productTable, countryipTable, countrynameTable);
    }

    /**
     * 
     * Select top 10 countries with the highest money spending
     * 
     * @param productTable     products table name
     * @param countrynameTable country names table name
     * 
     * @return sql query [sum, count, geoname id, country name]
     */
    public static String task_63(String productTable, String countrynameTable) {
        return String.format(
                "SELECT SUM(tp.price) as summ, count(*) as cnt, tcn.geonameId, tcn.countryName "
                        + "FROM (SELECT price, IP, IPAsLong, getGeoId(IPAsLong) as geoId FROM %s) tp "
                        + "INNER JOIN %s tcn ON tp.geoId = tcn.geonameId "
                        + "GROUP BY tcn.geonameId, tcn.countryName ORDER BY summ DESC LIMIT 10",
                productTable, countrynameTable);
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
        } catch (SQLException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        //
        // become a record in RDD
        //

        // Define Spark Configuration
        SparkConf conf = new SparkConf().setAppName("Getting-Started").setMaster("local[*]");

        // Create Spark Context with configuration
        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        final String PRODUCT = "product";
        final String COUNTRY_IP = "countryip";
        final String COUNTRY_NAME = "countryname";

        // Register the DataFrames as a temporary views
        spark.createDataFrame(
                sc.textFile(PRODUCT_PATH).map(f -> new Product(f.split(","))),
                Product.class)
                .createOrReplaceTempView(PRODUCT);

        System.out.println("Table product");
        spark.sql("SELECT price, IP, IPAsLong "
                + "FROM product ORDER BY price LIMIT 3").show();

        long startTime = 0;

        //
        // 5.1
        //
        System.out.println("Select top 10  most frequently purchased categories:");
        startTime = System.currentTimeMillis();
        Dataset<Row> df_51 = spark.sql(task_51(PRODUCT));
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
        Dataset<Row> df_52 = spark.sql(task_52(PRODUCT));
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
        // 6.3 with country name (Fast)
        //

        setupUDFs(spark, COUNTRYIP_PATH);

        Dataset<Row> dfCountryName = spark.createDataFrame(
                sc.textFile(COUNTRYNAME_PATH).map(f -> new CountryName(f.split(","))),
                CountryName.class);
        dfCountryName.cache().createOrReplaceTempView(COUNTRY_NAME);

        System.out.println("Select top 10 countries with the highest money spending (Fast)");
        startTime = System.currentTimeMillis();
        Dataset<Row> df_63 = spark.sql(task_63(PRODUCT, COUNTRY_NAME));
        df_63.cache().show();
        System.out.println(System.currentTimeMillis() - startTime);
        try {
            df_63.select("summ", "cnt", "geonameId", "countryName").write().mode(SaveMode.Overwrite).csv(OUT_63_PATH);
            df_63.write().mode(SaveMode.Overwrite).jdbc(MYSQL_CONNECTION_URL + MYSQL_DB, OUT_NAME_63,
                    connectionProperties);
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }

        //
        // 6.3 with country name (Slow)
        //

        Dataset<Row> dfCountryIP = spark.createDataFrame(
                sc.textFile(COUNTRYIP_PATH).map(f -> new CountryIP(f.split(","))),
                CountryIP.class);
        // Broadcast<List<Row>> broadcastIP = sc.broadcast(dfCountryIP.collectAsList());
        // Broadcast<StructType> broadcastSchemaIP = sc.broadcast(dfCountryIP.schema());
        // Dataset<Row> dfCountryIP_distributed =
        // spark.createDataFrame(broadcastIP.value(), broadcastSchemaIP.value());
        dfCountryIP.cache().createOrReplaceTempView(COUNTRY_IP);

        System.out.println("Select top 10 countries with the highest money spending (Slow)");
        startTime = System.currentTimeMillis();
        Dataset<Row> df_63a = spark.sql(task_63a(PRODUCT, COUNTRY_IP, COUNTRY_NAME));
        df_63a.cache().show();
        System.out.println(System.currentTimeMillis() - startTime);

        sc.close();
    }
}
