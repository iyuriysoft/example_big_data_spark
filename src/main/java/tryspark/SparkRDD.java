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
        JavaRDD<CountryName> rddCountryName = sc.textFile(COUNTRYNAME_PATH).map(f -> new CountryName(f.split(",")));
        JavaRDD<CountryIP> rddCountryIP = sc.textFile(COUNTRYIP_PATH).map(f -> new CountryIP(f.split(",")));

        //
        //
        // 5.1
        //
        //

        // I approach
        //
        System.out.println();
        System.out.println("5.1 Approach I: Using aggregateByKey - fastest");
        {
            JavaPairRDD<Integer, String> rdd51 = rddProduct.mapToPair(f -> new Tuple2<>(f.getCategory(), 1))
                    .aggregateByKey(0, (acc, v) -> acc + v, (acc1, acc2) -> acc1 + acc2)
                    .mapToPair(f -> new Tuple2<>(f._2, f._1)).sortByKey(false);
            rdd51.take(10).stream().forEach(a -> {
                System.out.println(a);
            });
        }

        // II approach
        //
        System.out.println();
        System.out.println("5.1 Approach II: Using groupBy, sortByKey - slower than I");
        {
            JavaPairRDD<Integer, String> rdd51 = rddProduct.groupBy(w -> w.getCategory().trim()).mapValues(f -> {
                return IterableUtils.size(f);
            }).mapToPair(f -> new Tuple2<>(f._2, f._1)).sortByKey(false);
            rdd51.take(10).stream().forEach(a -> {
                System.out.println(a);
            });
        }

        // III approach
        //
        // map(f -> f.swap()).sortBy(f -> f._1, false, 1)
        System.out.println();
        System.out.println("5.1 Approach III: Using groupBy, sortBy - slower than II");
        {
            JavaRDD<Tuple2<Integer, String>> rdd51a = rddProduct.groupBy(w -> w.getCategory().trim()).mapValues(f -> {
                return IterableUtils.size(f);
            }).map(f -> f.swap()).sortBy(f -> f._1, false, 1);
            rdd51a.take(10).stream().forEach(a -> {
                System.out.println(a);
            });

            // save to database
            StructType schema = DataTypes
                    .createStructType(Arrays.asList(DataTypes.createStructField("category", DataTypes.StringType, true),
                            DataTypes.createStructField("cnt", DataTypes.IntegerType, true)));
            JavaRDD<Row> rddRow = rdd51a.map((Tuple2<Integer, String> row) -> RowFactory.create(row._2, row._1));
            spark.createDataFrame(rddRow, schema).write().mode(SaveMode.Overwrite).jdbc(MYSQL_CONNECTION_URL + MYSQL_DB,
                    OUT_NAME_51, connectionProperties);
            // System.out.println(rdd51a.toDebugString());
        }

        //
        //
        // 5.2
        //
        //

        // Approach I
        //
        System.out.println();
        System.out.println("5.2 approach I, aggregateByKey, sortByKey - fastest");
        {
            JavaPairRDD<Integer, Tuple2<String, String>> rdd52a = rddProduct
                    .mapToPair(f -> new Tuple2<>(new Tuple2<>(f.getCategory(), f.getName()), 1))
                    .aggregateByKey(0, (acc, v) -> acc + v, (acc1, acc2) -> acc1 + acc2)
                    .mapToPair(f -> new Tuple2<>(f._2, f._1)).sortByKey(false);
            rdd52a.take(10).stream().forEach(a -> {
                System.out.println(a);
            });
        }

        // Approach II
        //
        System.out.println();
        System.out.println("5.2 approach II, groupBy, sortByKey - slower than I");
        {
            JavaPairRDD<Integer, Tuple2<String, String>> rdd52a = rddProduct.groupBy(w -> {
                return new Tuple2<>(w.getCategory().trim(), w.getName().trim());
            }).mapValues(f -> {
                return IterableUtils.size(f);
            }).mapToPair(f -> new Tuple2<>(f._2, f._1)).sortByKey(false);
            rdd52a.take(10).stream().forEach(a -> {
                System.out.println(a);
            });
        }

        // Approach III
        //
        System.out.println();
        System.out.println("5.2 approach III, groupBy, sortBy - slower than II");
        {
            JavaRDD<Tuple2<Integer, Tuple2<String, String>>> rdd52a = rddProduct.groupBy(w -> {
                return new Tuple2<>(w.getCategory().trim(), w.getName().trim());
            }).mapValues(f -> {
                return IterableUtils.size(f);
            }).map(f -> f.swap()).sortBy(f -> f._1, false, 1);
            rdd52a.take(10).stream().forEach(a -> {
                System.out.println(a);
            });

            // save to database
            StructType schema = DataTypes
                    .createStructType(Arrays.asList(DataTypes.createStructField("category", DataTypes.StringType, true),
                            DataTypes.createStructField("name", DataTypes.StringType, true),
                            DataTypes.createStructField("cnt", DataTypes.IntegerType, true)));
            JavaRDD<Row> rddRow = rdd52a.map(
                    (Tuple2<Integer, Tuple2<String, String>> row) -> RowFactory.create(row._2._1, row._2._2, row._1));
            spark.createDataFrame(rddRow, schema).write().mode(SaveMode.Overwrite).jdbc(MYSQL_CONNECTION_URL + MYSQL_DB,
                    OUT_NAME_52, connectionProperties);
        }

        //
        //
        // 6.1
        //
        //
        // SELECT t.ip, sum(t.price) sump FROM product t GROUP BY t.ip ORDER BY sump
        // DESC LIMIT 10;

        System.out.println();
        System.out.println("6.1 approach I, aggragateByKey - fastest way");
        JavaPairRDD<Float, Long> rdd61a; // price <-> IP
        {
            rdd61a = rddProduct.mapToPair(f -> new Tuple2<>(f.getIPAsLong(), f.getPriceAsFloat()))
                    .aggregateByKey(0.0f, (acc, v) -> v, (acc1, acc2) -> acc1 + acc2)
                    .mapToPair(f -> new Tuple2<>(f._2, f._1)).sortByKey(false);
            rdd61a.take(10).forEach(a -> {
                System.out.println(a);
            });
        }

        System.out.println();
        System.out.println("6.1 approach II, slow way");
        {
            rdd61a = rddProduct.groupBy(w -> w.getIPAsLong()).mapValues(f -> {
                float c = 0;
                for (Product p : f) {
                    c = c + p.getPriceAsFloat();
                }
                return c;
            }).mapToPair(f -> new Tuple2<>(f._2, f._1)).sortByKey(false);
            rdd61a.take(10).forEach(a -> {
                System.out.println(a);
            });
        }

        // limit only 10 elements for join query
        rdd61a = sc.parallelize(rdd61a.take(10)).mapToPair((x) -> new Tuple2<Float, Long>(x._1, x._2));

        System.out.println();
        System.out.println("6.1 with join");
        {
            // join countryIP & countryName
            JavaPairRDD<Long, CountryIP> aIP = rddCountryIP.mapToPair(f -> new Tuple2<>(f.getGeonameId(), f));
            JavaPairRDD<Long, CountryName> aName = rddCountryName.mapToPair(f -> new Tuple2<>(f.getGeonameId(), f));
            JavaPairRDD<Long, Tuple2<CountryIP, CountryName>> aJoin = aIP.join(aName);
            aJoin.cache();
            // cross Countries with products
            JavaPairRDD<Tuple2<Float, Long>, Tuple2<Long, Tuple2<CountryIP, CountryName>>> rdd61b = rdd61a
                    .cartesian(aJoin)
                    .filter(f -> f._1._2 > f._2._2._1.getStartIPAsLong() && f._1._2 < f._2._2._1.getEndIPAsLong());
            rdd61b.cache();
            // sort
            JavaPairRDD<Float, Tuple2<Tuple2<Float, Long>, Tuple2<Long, Tuple2<CountryIP, CountryName>>>> rdd61c = rdd61b
                    .mapToPair(f -> new Tuple2<>(f._1._1, new Tuple2<>(f._1, f._2))).sortByKey(false);

            rdd61c.cache();
            System.out.println();
            System.out.println("sorted:");
            rdd61c.take(10).forEach(a -> {
                System.out.println(String.format("%.1f %d %s %s", a._1, a._2._2._2._2.getGeonameId(),
                        a._2._2._2._2.getCountryName(), a._2._2._2._1.getNetwork()));
            });

            // save to database
            StructType schema = DataTypes
                    .createStructType(Arrays.asList(DataTypes.createStructField("sump", DataTypes.FloatType, true),
                            DataTypes.createStructField("geonameId", DataTypes.LongType, true),
                            DataTypes.createStructField("countryName", DataTypes.StringType, true),
                            DataTypes.createStructField("Network", DataTypes.StringType, true)));
            JavaRDD<Row> rddRow = rdd61b
                    .map((Tuple2<Tuple2<Float, Long>, Tuple2<Long, Tuple2<CountryIP, CountryName>>> row) -> RowFactory
                            .create(row._1._1, row._1._2, row._2._2._2.getCountryName(), row._2._2._1.getNetwork()));
            spark.createDataFrame(rddRow, schema).write().mode(SaveMode.Overwrite).jdbc(MYSQL_CONNECTION_URL + MYSQL_DB,
                    OUT_NAME_63, connectionProperties);

        }

        System.out.println("That's it");
        sc.close();
    }
}
