package tryspark;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.collections4.IterableUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;
import schema.CountryIP;
import schema.CountryName;
import schema.Product;
import udf.UDFGetGeoID;

//5.1 Approach I: Using aggregateByKey - fastest
//(163,category9)
//(162,category2)
//(162,category3)
//(162,category5)
//(159,category12)
//(156,category10)
//(155,category18)
//(154,category16)
//(151,category8)
//(150,category7)
//
//5.1 Approach II: Using groupBy, sortByKey - slower than I
//(163,category9)
//(162,category2)
//(162,category3)
//(162,category5)
//(159,category12)
//(156,category10)
//(155,category18)
//(154,category16)
//(151,category8)
//(150,category7)
//
//5.1 Approach III: Using groupBy, sortBy - slower than II
//(163,category9)
//(162,category2)
//(162,category3)
//(162,category5)
//(159,category12)
//(156,category10)
//(155,category18)
//(154,category16)
//(151,category8)
//(150,category7)
//
//5.2 approach I, aggregateByKey, sortByKey - fastest
//(16,(category0,product16))
//(16,(category3,product16))
//(15,(category9,product9))
//(15,(category3,product10))
//(14,(category3,product4))
//(14,(category16,product16))
//(14,(category18,product10))
//(14,(category10,product1))
//(14,(category18,product18))
//(14,(category12,product19))
//
//5.2 approach II, groupBy, sortByKey - slower than I
//(16,(category0,product16))
//(16,(category3,product16))
//(15,(category9,product9))
//(15,(category3,product10))
//(14,(category3,product4))
//(14,(category16,product16))
//(14,(category18,product10))
//(14,(category10,product1))
//(14,(category18,product18))
//(14,(category12,product19))
//
//5.2 approach III, groupBy, sortBy - slower than II
//(16,(category0,product16))
//(16,(category3,product16))
//(15,(category9,product9))
//(15,(category3,product10))
//(14,(category3,product4))
//(14,(category16,product16))
//(14,(category18,product10))
//(14,(category10,product1))
//(14,(category18,product18))
//(14,(category12,product19))
//
//6.3 approach II, using cartesian(), 
//
//559990.9 6252001 "United States"
//126911.9 1814991 China
//71033.4 1861060 Japan
//46460.1 2921044 Germany
//41081.2 2635167 "United Kingdom"
//40283.2 1835841 "Republic of Korea"
//32557.2 3017382 France
//29975.2 6251999 Canada
//28451.2 3469034 Brazil
//25028.1 3175395 Italy
//That's it

public class SparkRDD {
    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static String MYSQL_DB = "dbo";
    private static String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost/";
    private static String MYSQL_USERNAME = "root";
    private static String MYSQL_PWD = "password";

    private static final String DATA_PATH = "/Users/Shared/test/";
    private static final String INP_PRODUCT = "input3000.txt";
    private static final String INP_COUNTRYIP = "CountryIP.csv";
    private static final String INP_COUNTRYNAME = "CountryName.csv";
    private static final String OUT_NAME_51 = "table51";
    private static final String OUT_NAME_52 = "table52";
    private static final String OUT_NAME_63 = "table63";

    private static String PRODUCT_PATH = DATA_PATH + INP_PRODUCT;
    public static String COUNTRYIP_PATH = DATA_PATH + INP_COUNTRYIP;
    private static String COUNTRYNAME_PATH = DATA_PATH + INP_COUNTRYNAME;

    /**
     * creates database if not exists
     * 
     * @param dbname database name
     * @return Connection
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private static Connection prepareMySql(String dbname) throws ClassNotFoundException, SQLException {
        Class.forName(MYSQL_DRIVER);
        System.out.println("Connecting to database...");
        Connection conn = DriverManager.getConnection(MYSQL_CONNECTION_URL, MYSQL_USERNAME, MYSQL_PWD);
        System.out.println("Creating database...");
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbname);
        stmt.close();
        System.out.println("Database created successful");
        return conn;
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
     * task 51; approach 1; using aggregateByKey(); fastest way
     * 
     * @param rddProduct JavaRDD
     * @return JavaPairRDD<(records count), (category name)>
     */
    public static JavaPairRDD<Integer, String> task_51_approach_1(JavaRDD<Product> rddProduct) {
        return rddProduct.mapToPair(f -> new Tuple2<>(f.getCategory(), 1))
                .aggregateByKey(0, (acc, v) -> acc + v, (acc1, acc2) -> acc1 + acc2)
                .mapToPair(f -> new Tuple2<>(f._2, f._1)).sortByKey(false);
    }

    /**
     * task 51; approach 2; using operator groupBy(), SortByKey()
     * 
     * @param rddProduct JavaRDD
     * @return JavaPairRDD<(records count), (category name)>
     */
    private static JavaPairRDD<Integer, String> task_51_approach_2(JavaRDD<Product> rddProduct) {
        return rddProduct.groupBy(w -> w.getCategory().trim()).mapValues(f -> {
            return IterableUtils.size(f);
        }).mapToPair(f -> new Tuple2<>(f._2, f._1)).sortByKey(false);
    }

    /**
     * task 51; approach 3; using operator groupBy(), SortBy()
     * 
     * @param rddProduct JavaRDD
     * @return JavaPairRDD<(records count), (category name)>
     */
    private static JavaPairRDD<Integer, String> task_51_approach_3(JavaRDD<Product> rddProduct) {
        return rddProduct.groupBy(w -> w.getCategory().trim()).mapValues(f -> {
            return IterableUtils.size(f);
        }).map(f -> f.swap()).sortBy(f -> f._1, false, 1).mapToPair(f -> new Tuple2<>(f._1, f._2));
    }

    /**
     * task 52; approach 1; using aggregateByKey; fastest way
     * 
     * @param rddProduct JavaRDD
     * @return JavaPairRDD<(products count in the category), Tuple2<(product name),
     *         (category name)>>
     */
    public static JavaPairRDD<Integer, Tuple2<String, String>> task_52_approach_1(JavaRDD<Product> rddProduct) {
        return rddProduct.mapToPair(f -> new Tuple2<>(new Tuple2<>(f.getCategory(), f.getName()), 1))
                .aggregateByKey(0, (acc, v) -> acc + v, (acc1, acc2) -> acc1 + acc2)
                .mapToPair(f -> new Tuple2<>(f._2, f._1)).sortByKey(false);
    }

    /**
     * task 52; approach 2; using groupBy(), SortByKey()
     * 
     * @param rddProduct JavaRDD
     * @return JavaPairRDD<(products count in the category), Tuple2<(product name),
     *         (category name)>>
     */
    private static JavaPairRDD<Integer, Tuple2<String, String>> task_52_approach_2(JavaRDD<Product> rddProduct) {
        return rddProduct.groupBy(w -> {
            return new Tuple2<>(w.getCategory().trim(), w.getName().trim());
        }).mapValues(f -> {
            return IterableUtils.size(f);
        }).mapToPair(f -> new Tuple2<>(f._2, f._1)).sortByKey(false);
    }

    /**
     * task 52; approach 3; using groupBy(), SortBy()
     * 
     * @param rddProduct JavaRDD
     * @return JavaPairRDD<(products count in the category), Tuple2<(product name),
     *         (category name)>>
     */
    private static JavaPairRDD<Integer, Tuple2<String, String>> task_52_approach_3(JavaRDD<Product> rddProduct) {
        return rddProduct.groupBy(w -> {
            return new Tuple2<>(w.getCategory().trim(), w.getName().trim());
        }).mapValues(f -> {
            return IterableUtils.size(f);
        }).map(f -> f.swap()).sortBy(f -> f._1, false, 1).mapToPair(f -> new Tuple2<>(f._1, f._2));
    }

    /**
     * decide task N51 different approaches and put the result in the database
     * 
     * @param rddProduct           JavaRDD
     * @param spark                session
     * @param connectionProperties MySql properties
     */
    private static void doTask_51(JavaRDD<Product> rddProduct, SparkSession spark, Properties connectionProperties) {

        long startTime = 0;
        JavaPairRDD<Integer, String> rdd51;

        // I approach
        //
        System.out.println();
        System.out.println("5.1 Approach I: Using aggregateByKey - fastest");
        startTime = System.currentTimeMillis();
        rdd51 = task_51_approach_1(rddProduct);
        rdd51.take(10).stream().forEach(a -> {
            System.out.println(a);
        });
        System.out.println(System.currentTimeMillis() - startTime);

        // II approach
        //
        System.out.println();
        System.out.println("5.1 Approach II: Using groupBy, sortByKey - slower than I");
        startTime = System.currentTimeMillis();
        rdd51 = task_51_approach_2(rddProduct);
        rdd51.take(10).stream().forEach(a -> {
            System.out.println(a);
        });
        System.out.println(System.currentTimeMillis() - startTime);

        // III approach
        //
        System.out.println();
        System.out.println("5.1 Approach III: Using groupBy, sortBy - slower than II");
        startTime = System.currentTimeMillis();
        rdd51 = task_51_approach_3(rddProduct);
        rdd51.take(10).stream().forEach(a -> {
            System.out.println(a);
        });
        System.out.println(System.currentTimeMillis() - startTime);

        // save to database
        try {
            StructType schema = DataTypes
                    .createStructType(Arrays.asList(DataTypes.createStructField("category", DataTypes.StringType, true),
                            DataTypes.createStructField("cnt", DataTypes.IntegerType, true)));
            JavaRDD<Row> rddRow = rdd51.map((Tuple2<Integer, String> row) -> RowFactory.create(row._2, row._1));
            spark.createDataFrame(rddRow, schema).write().mode(SaveMode.Overwrite).jdbc(MYSQL_CONNECTION_URL + MYSQL_DB,
                    OUT_NAME_51, connectionProperties);
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }

    }

    /**
     * decide task N52 different approaches and put the result in the database
     *
     * @param rddProduct           JavaRDD
     * @param spark                session
     * @param connectionProperties MySql properties
     */
    private static void doTask_52(JavaRDD<Product> rddProduct, SparkSession spark, Properties connectionProperties) {

        long startTime = 0;
        JavaPairRDD<Integer, Tuple2<String, String>> rdd52;

        // Approach I
        //
        System.out.println();
        System.out.println("5.2 approach I, aggregateByKey, sortByKey - fastest");
        startTime = System.currentTimeMillis();
        rdd52 = task_52_approach_1(rddProduct);
        rdd52.take(10).stream().forEach(a -> {
            System.out.println(a);
        });
        System.out.println(System.currentTimeMillis() - startTime);

        // Approach II
        //
        System.out.println();
        System.out.println("5.2 approach II, groupBy, sortByKey - slower than I");
        startTime = System.currentTimeMillis();
        rdd52 = task_52_approach_2(rddProduct);
        rdd52.take(10).stream().forEach(a -> {
            System.out.println(a);
        });
        System.out.println(System.currentTimeMillis() - startTime);

        // Approach III
        //
        System.out.println();
        System.out.println("5.2 approach III, groupBy, sortBy - slower than II");
        startTime = System.currentTimeMillis();
        rdd52 = task_52_approach_3(rddProduct);
        rdd52.take(10).stream().forEach(a -> {
            System.out.println(a);
        });
        System.out.println(System.currentTimeMillis() - startTime);

        // save to database
        try {
            StructType schema = DataTypes
                    .createStructType(Arrays.asList(DataTypes.createStructField("category", DataTypes.StringType, true),
                            DataTypes.createStructField("name", DataTypes.StringType, true),
                            DataTypes.createStructField("cnt", DataTypes.IntegerType, true)));
            JavaRDD<Row> rddRow = rdd52.map(
                    (Tuple2<Integer, Tuple2<String, String>> row) -> RowFactory.create(row._2._1, row._2._2, row._1));
            spark.createDataFrame(rddRow, schema).write().mode(SaveMode.Overwrite).jdbc(MYSQL_CONNECTION_URL + MYSQL_DB,
                    OUT_NAME_52, connectionProperties);
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }

    }

    /**
     * task 63; approach 2; using cartesian() to cross Countries with Products
     * (slow)
     * 
     * @param rddProduct       JavaRDD
     * @param rddCountryNameIP JavaPairRDD<(geo name id), Tuple2<(Object CountryIP),
     *                         (Object CountryName)>>
     * @param sc               Java Spark Context
     * @return JavaPairRDD<(sum of prices), Tuple2<(country Id), (country Name)>>
     */
    public static JavaPairRDD<Float, Tuple2<Long, String>> task_63_approach_2(
            JavaRDD<Product> rddProduct,
            JavaPairRDD<Long, Tuple2<CountryIP, CountryName>> rddCountryNameIP,
            JavaSparkContext sc) {
        return rddProduct
                .mapToPair(f -> new Tuple2<>(f.getPriceAsFloat(), f.getIPAsLong()))
                .cache().cartesian(rddCountryNameIP)
                .filter(f -> f._1._2 > f._2._2._1.getStartIPAsLong() && f._1._2 < f._2._2._1.getEndIPAsLong())
                .mapToPair(f -> new Tuple2<>(f._2._1, f))
                .combineByKey(x -> new Tuple2<Float, String>(x._1._1, x._2._2._2.getCountryName()),
                        (c1, v) -> new Tuple2<Float, String>(c1._1 + v._1._1, v._2._2._2.getCountryName()),
                        (c1, c2) -> new Tuple2<>(c1._1 + c2._1, c2._2))
                .mapToPair(f -> new Tuple2<>(f._2._1, new Tuple2<>(f._1, f._2._2)));
    }

    /**
     * task 63; approach 1; using function (fast way)
     * 
     * @param rddProduct     JavaRDD
     * @param rddCountryName JavaPairRDD<(geo name id), (Object CountryName)>
     * @param sc             Java Spark Context
     * @return JavaPairRDD<(sum of prices), Tuple2<(country Id), (country Name)>>
     */
    public static JavaPairRDD<Float, Tuple2<Long, String>> task_63_approach_1(
            JavaRDD<Product> rddProduct,
            JavaPairRDD<Long, CountryName> rddCountryName,
            JavaSparkContext sc) {

        JavaPairRDD<Long, Tuple2<Float, String>> r1 = rddProduct
                .mapToPair(f -> new Tuple2<>(UDFGetGeoID.ip2geo(f.getIPAsLong()), f))
                .cache()
                .join(rddCountryName)
                .mapToPair(f -> new Tuple2<>(
                        f._1, new Tuple2<>(f._2._1.getPriceAsFloat(), f._2._2.getCountryName())))
                .combineByKey(x -> new Tuple2<Float, String>(x._1, x._2),
                        (c1, v) -> new Tuple2<Float, String>(c1._1 + v._1, v._2),
                        (c1, c2) -> new Tuple2<>(c1._1 + c2._1, c2._2));
        JavaPairRDD<Float, Tuple2<Long, String>> r2 = r1
                .mapToPair(f -> new Tuple2<>(f._2._1, new Tuple2<>(f._1, f._2._2)));
        return r2;
    }

    /**
     * decide task N63 with country names
     * 
     * @param rddProduct           JavaRDD
     * @param rddCountryName       JavaPairRDD<(geo name id), (Object Country Name),
     * @param rddCountryNameIP     JavaPairRDD<(geo name id), Tuple2<(Object Country
     *                             IP), (ObjectCountry Name)>>
     * @param spark                spark session
     * @param connectionProperties MySql properties
     * 
     * @throws FileNotFoundException
     * @throws IOException
     */
    private static void doTask_63(JavaRDD<Product> rddProduct,
            JavaPairRDD<Long, CountryName> rddCountryName,
            JavaPairRDD<Long, Tuple2<CountryIP, CountryName>> rddCountryNameIP,
            SparkSession spark,
            Properties connectionProperties) throws FileNotFoundException, IOException {

        long startTime = 0;
        JavaPairRDD<Float, Tuple2<Long, String>> rdd63b = null;

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

        System.out.println();
        System.out.println("6.3 approach I (fast way)");
        {
            UDFGetGeoID.init(COUNTRYIP_PATH, sc);
            startTime = System.currentTimeMillis();
            rdd63b = task_63_approach_1(rddProduct, rddCountryName, sc);
            rdd63b.sortByKey(false).take(10)
                    .forEach(a -> {
                        System.out.println(String.format("%.1f %d %s", a._1, a._2._1, a._2._2));
                    });
            System.out.println(System.currentTimeMillis() - startTime);
        }

        System.out.println();
        System.out.println("6.3 approach II, using cartesian(), (slow)");
        {
            startTime = System.currentTimeMillis();
            rdd63b = task_63_approach_2(rddProduct, rddCountryNameIP, sc);
            rdd63b.sortByKey(false).take(10)
                    .forEach(a -> {
                        System.out.println(String.format("%.1f %d %s", a._1, a._2._1, a._2._2));
                    });
            //// rdd limit
            // rdd63b = sc.parallelize(rdd63b.sortByKey(false).take(10)).mapToPair((x) ->
            //// new Tuple2<>(x._1, x._2));
            // rdd63b.cache();
            //// show in sorted view
            // rdd63b.take(10)
            // .forEach(a -> {
            // System.out.println(String.format("%.1f %d %s", a._1, a._2._1, a._2._2));
            // });
            System.out.println(System.currentTimeMillis() - startTime);
        }

        // save to database
        try {
            StructType schema = DataTypes
                    .createStructType(Arrays.asList(DataTypes.createStructField("sump", DataTypes.FloatType, true),
                            DataTypes.createStructField("geonameId", DataTypes.LongType, true),
                            DataTypes.createStructField("countryName", DataTypes.StringType, true)));
            JavaRDD<Row> rddRow = rdd63b
                    .map((Tuple2<Float, Tuple2<Long, String>> row) -> RowFactory
                            .create(row._1, row._2._1, row._2._2));
            spark.createDataFrame(rddRow, schema).write().mode(SaveMode.Overwrite).jdbc(MYSQL_CONNECTION_URL + MYSQL_DB,
                    OUT_NAME_63, connectionProperties);
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }

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

        // Define Spark Configuration
        SparkConf conf = new SparkConf().setAppName("01-Getting-Started").setMaster("local[*]")
        // .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        // .set("spark.kryo.registrationRequired", "true")
        ;

        // Create Spark Context with configuration
        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

        //
        // load data
        //

        JavaRDD<Product> rddProduct = sc.textFile(PRODUCT_PATH).map(f -> new Product(f.split(",")));
        rddProduct.cache();

        //
        // 5.1 Select top 10 most frequently purchased categories
        //
        doTask_51(rddProduct, spark, connectionProperties);

        //
        // 5.2 Select top 10 most frequently purchased product in each category
        //
        doTask_52(rddProduct, spark, connectionProperties);

        //
        // 6.3 Select top 10 countries with the highest money spending
        //

        // load country names
        JavaRDD<CountryName> rddCountryName = sc.textFile(COUNTRYNAME_PATH)
                .map(f -> new CountryName(f.split(",")));
        JavaPairRDD<Long, CountryName> rddName = rddCountryName
                .mapToPair(f -> new Tuple2<>(f.getGeonameId(), f))
                .cache();

        // load country ip
        JavaRDD<CountryIP> rddCountryIP = sc.textFile(COUNTRYIP_PATH)
                .map(f -> new CountryIP(f.split(",")));
        JavaPairRDD<Long, CountryIP> aIP = rddCountryIP
                .mapToPair(f -> new Tuple2<>(f.getGeonameId(), f));
        JavaPairRDD<Long, Tuple2<CountryIP, CountryName>> rddCountryNameIP = aIP.join(rddName).cache();

        // show countries
        doTask_63(rddProduct, rddName, rddCountryNameIP, spark, connectionProperties);

        System.out.println("That's it");
        sc.close();
    }
}
