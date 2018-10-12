package tryspark;

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
//6.1 
//(534.0,4177887238)
//(533.5,3609693019)
//(532.1,4182146946)
//(531.5,644197344)
//(531.4,468617227)
//(531.1,1411779490)
//(530.8,1949334692)
//(530.3,337244083)
//(528.6,1050574175)
//(528.3,2962904592)
//
//6.1 with join
//sorted:
//533.5 6252001 "United States" 215.32.0.0/11
//531.5 6252001 "United States" 38.101.128.0/18
//531.4 1835841 "Republic of Korea" 27.232.0.0/13
//531.1 3144096 Norway 84.38.8.0/21
//530.8 1819730 "Hong Kong" 116.48.0.0/15
//530.3 6252001 "United States" 20.0.0.0/11
//528.6 2921044 Germany 62.158.0.0/16
//528.3 3017382 France 176.128.0.0/10
//That's it

public class SparkRDD {
    private static final String MYSQL_DB = "dbo";
    private static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static final String MYSQL_CONNECTION_URL = "jdbc:mysql://localhost/";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PWD = "password";

    private static final String DATA_PATH = "/Users/Shared/test/";
    private static final String INP_PRODUCT = "input3000.txt";
    private static final String INP_COUNTRYIP = "CountryIP.csv";
    private static final String INP_COUNTRYNAME = "CountryName.csv";
    private static final String OUT_NAME_51 = "table51";
    private static final String OUT_NAME_52 = "table52";
    private static final String OUT_NAME_63 = "table63";

    private static final String PRODUCT_PATH = DATA_PATH + INP_PRODUCT;
    private static final String COUNTRYIP_PATH = DATA_PATH + INP_COUNTRYIP;
    private static final String COUNTRYNAME_PATH = DATA_PATH + INP_COUNTRYNAME;

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
     * task 51; approach 1; using aggregateByKey(); fastest way
     * 
     * @param rddProduct JavaRDD
     * @return JavaPairRDD<(records count), (category name)>
     */
    private static JavaPairRDD<Integer, String> task_51_approach_1(JavaRDD<Product> rddProduct) {
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
    private static JavaPairRDD<Integer, Tuple2<String, String>> task_52_approach_1(JavaRDD<Product> rddProduct) {
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
     * task 63; Only ip; approach 1; using aggregateByKey(); fastest way
     * 
     * @param rddProduct JavaRDD
     * @return JavaPairRDD<(sum of prices), (IP as Long)>
     */
    private static JavaPairRDD<Float, Long> task_63ip_approach_1(JavaRDD<Product> rddProduct) {
        return rddProduct.mapToPair(f -> new Tuple2<>(f.getIPAsLong(), f.getPriceAsFloat()))
                .aggregateByKey(0.0f, (acc, v) -> acc + v, (acc1, acc2) -> acc1 + acc2)
                .mapToPair(f -> new Tuple2<>(f._2, f._1)).sortByKey(false);
    }

    /**
     * task 63; Only ip; approach 2; using groupBy(), SortByKey()
     * 
     * @param rddProduct JavaRDD
     * @return JavaPairRDD<(sum of prices), (IP as Long)>
     */
    private static JavaPairRDD<Float, Long> task_63ip_approach_2(JavaRDD<Product> rddProduct) {
        return rddProduct.groupBy(w -> w.getIPAsLong()).mapValues(f -> {
            float c = 0;
            for (Product p : f) {
                c = c + p.getPriceAsFloat();
            }
            return c;
        }).mapToPair(f -> new Tuple2<>(f._2, f._1)).sortByKey(false);
    }

    /**
     * task 63; approach 1 cross Countries with Products
     * 
     * @param rddProduct       JavaRDD
     * @param rddCountryNameIP JavaPairRDD<(Country id), Tuple2<(Country IP),
     *                         (Country Name)>>
     * @return JavaPairRDD<Tuple2<(sum of prices), (IP as Long)>, Tuple2<(Country
     *         Id), Tuple2<(Country IP), (Country Name)>>>
     */
    private static JavaPairRDD<Tuple2<Float, Long>, Tuple2<Long, Tuple2<CountryIP, CountryName>>> task_63_approach_1(
            JavaRDD<Product> rddProduct, JavaPairRDD<Long, Tuple2<CountryIP, CountryName>> rddCountryNameIP) {

        JavaPairRDD<Tuple2<Float, Long>, Tuple2<Long, Tuple2<CountryIP, CountryName>>> rdd63b = null;
        JavaPairRDD<Float, Long> rdd63 = rddProduct.mapToPair(f -> new Tuple2<>(f.getIPAsLong(), f.getPriceAsFloat()))
                .aggregateByKey(0.0f, (acc, v) -> acc + v, (acc1, acc2) -> acc1 + acc2)
                .mapToPair(f -> new Tuple2<>(f._2, f._1)).sortByKey(false);
        // get Country for every ip
        for (Tuple2<Float, Long> it : rdd63.take(10)) {
            JavaPairRDD<Tuple2<Float, Long>, Tuple2<Long, Tuple2<CountryIP, CountryName>>> rdd = rddCountryNameIP
                    .filter(f -> it._2 > f._2._1.getStartIPAsLong() && it._2 < f._2._1.getEndIPAsLong())
                    .mapToPair(f -> new Tuple2<>(it, f));
            rdd63b = rdd63b == null ? rdd : rdd63b.union(rdd);
        }
        return rdd63b;
    }

    /**
     * task 63; approach 2; using cartesian() cross Countries with Products
     * 
     * @param rddProduct       JavaRDD
     * @param rddCountryNameIP JavaPairRDD<(Country id), Tuple2<(Country IP),
     *                         (Country Name)>>
     * @param sc               Java Spark Context
     * @return JavaPairRDD<Tuple2<(sum of prices), (IP as Long)>, Tuple2<(Country
     *         Id), Tuple2<(Country IP), (Country Name)>>>
     */
    private static JavaPairRDD<Tuple2<Float, Long>, Tuple2<Long, Tuple2<CountryIP, CountryName>>> task_63_approach_2(
            JavaRDD<Product> rddProduct, JavaPairRDD<Long, Tuple2<CountryIP, CountryName>> rddCountryNameIP,
            JavaSparkContext sc) {

        JavaPairRDD<Float, Long> rdd63a = rddProduct.mapToPair(f -> new Tuple2<>(f.getIPAsLong(), f.getPriceAsFloat()))
                .aggregateByKey(0.0f, (acc, v) -> acc + v, (acc1, acc2) -> acc1 + acc2)
                .mapToPair(f -> new Tuple2<>(f._2, f._1)).sortByKey(false);
        // limit only 10 elements for join query to speed up
        rdd63a = sc.parallelize(rdd63a.take(10)).mapToPair((x) -> new Tuple2<Float, Long>(x._1, x._2));
        // cross Countries with products
        return rdd63a.cartesian(rddCountryNameIP)
                .filter(f -> f._1._2 > f._2._2._1.getStartIPAsLong() && f._1._2 < f._2._2._1.getEndIPAsLong());
    }

    /**
     * decides task N51 different approaches and put the result to database
     * 
     * @param rddProduct           JavaRDD
     * @param spark                session
     * @param connectionProperties MySql properties
     */
    private static void doTask_51(JavaRDD<Product> rddProduct, SparkSession spark, Properties connectionProperties) {

        JavaPairRDD<Integer, String> rdd51;

        // I approach
        //
        System.out.println();
        System.out.println("5.1 Approach I: Using aggregateByKey - fastest");
        rdd51 = task_51_approach_1(rddProduct);
        rdd51.take(10).stream().forEach(a -> {
            System.out.println(a);
        });

        // II approach
        //
        System.out.println();
        System.out.println("5.1 Approach II: Using groupBy, sortByKey - slower than I");
        rdd51 = task_51_approach_2(rddProduct);
        rdd51.take(10).stream().forEach(a -> {
            System.out.println(a);
        });

        // III approach
        //
        System.out.println();
        System.out.println("5.1 Approach III: Using groupBy, sortBy - slower than II");
        rdd51 = task_51_approach_3(rddProduct);
        rdd51.take(10).stream().forEach(a -> {
            System.out.println(a);
        });

        // save to database
        {
            StructType schema = DataTypes
                    .createStructType(Arrays.asList(DataTypes.createStructField("category", DataTypes.StringType, true),
                            DataTypes.createStructField("cnt", DataTypes.IntegerType, true)));
            JavaRDD<Row> rddRow = rdd51.map((Tuple2<Integer, String> row) -> RowFactory.create(row._2, row._1));
            spark.createDataFrame(rddRow, schema).write().mode(SaveMode.Overwrite).jdbc(MYSQL_CONNECTION_URL + MYSQL_DB,
                    OUT_NAME_51, connectionProperties);
        }
    }

    /**
     * decides task N52 different approaches and put the result to database
     *
     * @param rddProduct           JavaRDD
     * @param spark                session
     * @param connectionProperties MySql properties
     */
    private static void doTask_52(JavaRDD<Product> rddProduct, SparkSession spark, Properties connectionProperties) {

        JavaPairRDD<Integer, Tuple2<String, String>> rdd52;

        // Approach I
        //
        System.out.println();
        System.out.println("5.2 approach I, aggregateByKey, sortByKey - fastest");
        {
            rdd52 = task_52_approach_1(rddProduct);
            rdd52.take(10).stream().forEach(a -> {
                System.out.println(a);
            });
        }

        // Approach II
        //
        System.out.println();
        System.out.println("5.2 approach II, groupBy, sortByKey - slower than I");
        rdd52 = task_52_approach_2(rddProduct);
        rdd52.take(10).stream().forEach(a -> {
            System.out.println(a);
        });

        // Approach III
        //
        System.out.println();
        System.out.println("5.2 approach III, groupBy, sortBy - slower than II");
        rdd52 = task_52_approach_3(rddProduct);
        rdd52.take(10).stream().forEach(a -> {
            System.out.println(a);
        });

        // save to database
        {
            StructType schema = DataTypes
                    .createStructType(Arrays.asList(DataTypes.createStructField("category", DataTypes.StringType, true),
                            DataTypes.createStructField("name", DataTypes.StringType, true),
                            DataTypes.createStructField("cnt", DataTypes.IntegerType, true)));
            JavaRDD<Row> rddRow = rdd52.map(
                    (Tuple2<Integer, Tuple2<String, String>> row) -> RowFactory.create(row._2._1, row._2._2, row._1));
            spark.createDataFrame(rddRow, schema).write().mode(SaveMode.Overwrite).jdbc(MYSQL_CONNECTION_URL + MYSQL_DB,
                    OUT_NAME_52, connectionProperties);
        }
    }

    /**
     * decides task N63 but only ip without country names
     * 
     * @param rddProduct JavaRDD
     */
    private static void doTask_63_only_ip(JavaRDD<Product> rddProduct) {

        // SELECT t.ip, sum(t.price) sump
        // FROM product t GROUP BY t.ip ORDER BY sump DESC LIMIT 10;

        System.out.println();
        System.out.println("6.3 approach I, aggragateByKey - fastest way");
        task_63ip_approach_1(rddProduct).take(10).forEach(a -> {
            System.out.println(a);
        });

        System.out.println();
        System.out.println("6.3 approach II, slow way");
        task_63ip_approach_2(rddProduct).take(10).forEach(a -> {
            System.out.println(a);
        });
    }

    /**
     * decides task N63 with country names
     *
     * @param rddProduct           JavaRDD
     * @param rddCountryNameIP     JavaPairRDD<(Country id), Tuple2<(Country IP),
     *                             (Country Name)>>
     * @param spark                session
     * @param connectionProperties MySql properties
     * @param sc                   context
     */
    private static void doTask_63(JavaRDD<Product> rddProduct,
            JavaPairRDD<Long, Tuple2<CountryIP, CountryName>> rddCountryNameIP, SparkSession spark,
            Properties connectionProperties, JavaSparkContext sc) {

        JavaPairRDD<Tuple2<Float, Long>, Tuple2<Long, Tuple2<CountryIP, CountryName>>> rdd63b = null;

        System.out.println();
        System.out.println("6.3 approach I, using union()");
        {
            rdd63b = task_63_approach_1(rddProduct, rddCountryNameIP);
            rdd63b.cache();
            // show in sorted view
            rdd63b.mapToPair(f -> new Tuple2<>(f._1._1, new Tuple2<>(f._1, f._2))).sortByKey(false).take(10)
                    .forEach(a -> {
                        System.out.println(String.format("%.1f %d %s %s", a._1, a._2._2._2._2.getGeonameId(),
                                a._2._2._2._2.getCountryName(), a._2._2._2._1.getNetwork()));
                    });
        }

        System.out.println();
        System.out.println("6.3 approach II, using cartesian(), ");
        {
            rdd63b = task_63_approach_2(rddProduct, rddCountryNameIP, sc);
            rdd63b.cache();
            // show in sorted view
            rdd63b.mapToPair(f -> new Tuple2<>(f._1._1, new Tuple2<>(f._1, f._2))).sortByKey(false).take(10)
                    .forEach(a -> {
                        System.out.println(String.format("%.1f %d %s %s", a._1, a._2._2._2._2.getGeonameId(),
                                a._2._2._2._2.getCountryName(), a._2._2._2._1.getNetwork()));
                    });
        }

        // save to database
        {
            StructType schema = DataTypes
                    .createStructType(Arrays.asList(DataTypes.createStructField("sump", DataTypes.FloatType, true),
                            DataTypes.createStructField("geonameId", DataTypes.LongType, true),
                            DataTypes.createStructField("countryName", DataTypes.StringType, true),
                            DataTypes.createStructField("IP", DataTypes.StringType, true)));
            JavaRDD<Row> rddRow = rdd63b
                    .map((Tuple2<Tuple2<Float, Long>, Tuple2<Long, Tuple2<CountryIP, CountryName>>> row) -> RowFactory
                            .create(row._1._1, row._1._2, row._2._2._2.getCountryName(), row._2._2._1.getNetwork()));
            spark.createDataFrame(rddRow, schema).write().mode(SaveMode.Overwrite).jdbc(MYSQL_CONNECTION_URL + MYSQL_DB,
                    OUT_NAME_63, connectionProperties);
        }
    }

    public static void main(String args[]) throws ClassNotFoundException, SQLException {

        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", MYSQL_USERNAME);
        connectionProperties.put("password", MYSQL_PWD);
        prepareMySql(MYSQL_DB);

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
        JavaRDD<CountryName> rddCountryName = sc.textFile(COUNTRYNAME_PATH).map(f -> new CountryName(f.split(",")));
        rddCountryName.cache();
        JavaRDD<CountryIP> rddCountryIP = sc.textFile(COUNTRYIP_PATH).map(f -> new CountryIP(f.split(",")));
        rddCountryIP.cache();

        //
        // 5.1 Select top 10 most frequently purchased categories (using only Product)
        //
        doTask_51(rddProduct, spark, connectionProperties);

        //
        // 5.2 Select top 10 most frequently purchased product in each category (using only Product)
        //
        doTask_52(rddProduct, spark, connectionProperties);

        //
        // 6.3 Select top 10 countries with the highest money spending
        //

        // show only IP (without country names!)
        doTask_63_only_ip(rddProduct);

        // load countries data and join countryIP & countryName
        JavaPairRDD<Long, CountryIP> aIP = rddCountryIP.mapToPair(f -> new Tuple2<>(f.getGeonameId(), f));
        JavaPairRDD<Long, CountryName> aName = rddCountryName.mapToPair(f -> new Tuple2<>(f.getGeonameId(), f));
        JavaPairRDD<Long, Tuple2<CountryIP, CountryName>> rddCountryNameIP = aIP.join(aName);
        rddCountryNameIP.cache();

        // show countries
        doTask_63(rddProduct, rddCountryNameIP, spark, connectionProperties, sc);

        System.out.println("That's it");
        sc.close();
    }
}
