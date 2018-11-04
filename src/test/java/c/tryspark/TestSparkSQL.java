package c.tryspark;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import schema.CountryIP;
import schema.CountryName;
import schema.Product;
import tryspark.SparkSQL;

/**
 * Unit test for Spark SQL.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestSparkSQL {

    private static JavaSparkContext sparkCtx;
    private static SparkSession spark;
    private static Product[] arInput = new Product[] {
            new Product("product2, 504.8, 2010-10-20 14:01:47.520, category0, 12.68.11.1"),
            new Product("product2, 500.0, 2010-10-20 18:14:09.600, category0, 172.68.11.1"),
            new Product("product2, 507.0, 2010-10-21 17:06:05.888, category0, 172.68.11.22"),
            new Product("product2, 494.4, 2010-10-18 03:04:32.320, category0, 172.68.11.33"),
            new Product("product0, 498.6, 2010-10-17 11:19:30.528, category1, 172.68.11.44"),
            new Product("product0, 497.3, 2010-10-21 13:07:50.400, category1, 12.68.11.1"),
            new Product("product0, 506.7, 2010-10-16 22:15:59.248, category1, 12.68.11.1"),
            new Product("product2, 502.5, 2010-10-17 19:57:12.336, category1, 12.68.11.3"),
            new Product("product1, 499.8, 2010-10-20 00:02:05.280, category2, 1.2.3.5"),
            new Product("product4, 500.6, 2010-10-17 08:55:32.640, category3, 1.2.3.6"),
    };
    private static CountryName[] arCountryName = new CountryName[] {
            new CountryName("1,,,,,United States"),
            new CountryName("3,,,,,Republic of Korea"),
            new CountryName("6,,,,,Norway"),
    };
    private static CountryIP[] arCountryIP = new CountryIP[] {
            new CountryIP("172.68.11.0/24, 1"),
            new CountryIP("12.68.11.0/24, 3"),
            new CountryIP("1.2.3.0/27, 6"),
    };

    @BeforeClass
    public static void init() throws IllegalArgumentException, IOException {
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("junit");
        sparkCtx = new JavaSparkContext(conf);
        spark = SparkSession.builder().config(conf).getOrCreate();
    }

    @AfterClass
    public static void exit() {
        sparkCtx.close();
    }

    @Test
    public void test_Task51_1() {
        spark.createDataFrame(sparkCtx.parallelize(Arrays.asList(arInput)), Product.class)
                .createOrReplaceTempView("product");
        
        List<String> actuals = spark.sql(SparkSQL.task_51("product")).collectAsList().stream()
                .map(f -> String.format("%d %s", f.get(1), f.get(0)))
                .collect(Collectors.toList());
        List<String> expecteds = Arrays.asList("4 category0", "4 category1", "1 category2", "1 category3");
        expecteds.stream().forEach(f -> Assert.assertTrue(actuals.contains(f)));
        Assert.assertEquals(actuals.size(), 4);
    }

    @Test
    public void test_Task52_1() {
        spark.createDataFrame(sparkCtx.parallelize(Arrays.asList(arInput)), Product.class)
                .createOrReplaceTempView("product");
        
        List<String> actuals = spark.sql(SparkSQL.task_52("product")).collectAsList().stream()
                .map(f -> String.format("%d %s %s", f.get(2), f.get(1), f.get(0)))
                .collect(Collectors.toList());
        List<String> expecteds = Arrays.asList("4 category0 product2", "3 category1 product0", "1 category1 product2",
                "1 category3 product4", "1 category2 product1");
        expecteds.stream().forEach(f -> Assert.assertTrue(actuals.contains(f)));
        Assert.assertEquals(actuals.size(), 5);
        // actuals.stream().forEach(f -> System.out.println(f));
    }

    @Test
    public void test_Task63_1() throws FileNotFoundException, IOException {
        spark.createDataFrame(sparkCtx.parallelize(Arrays.asList(arInput)), Product.class)
                .createOrReplaceTempView("product");
        spark.createDataFrame(sparkCtx.parallelize(Arrays.asList(arCountryIP)), CountryIP.class)
                .createOrReplaceTempView("countryip");
        spark.createDataFrame(sparkCtx.parallelize(Arrays.asList(arCountryName)), CountryName.class)
                .createOrReplaceTempView("countryname");
        
        SparkSQL.setupUDFs(spark, arCountryIP);
        
        List<String> actuals = spark.sql(SparkSQL.task_63("product", "countryname")).collectAsList()
                .stream()
                .map(f -> String.format("%.1f %d %s", f.get(0), f.get(2), f.get(3)))
                .collect(Collectors.toList());
        List<String> expecteds = Arrays.asList("2000.0 1 United States", "1000.4 6 Norway",
                "2011.3 3 Republic of Korea");
        expecteds.stream().forEach(f -> Assert.assertTrue(actuals.contains(f)));
        Assert.assertEquals(actuals.size(), 3);
    }

    @Test
    public void test_Task63_2() {
        spark.createDataFrame(sparkCtx.parallelize(Arrays.asList(arInput)), Product.class)
                .createOrReplaceTempView("product");
        spark.createDataFrame(sparkCtx.parallelize(Arrays.asList(arCountryIP)), CountryIP.class)
                .createOrReplaceTempView("countryip");
        spark.createDataFrame(sparkCtx.parallelize(Arrays.asList(arCountryName)), CountryName.class)
                .createOrReplaceTempView("countryname");
        
        List<String> actuals = spark.sql(SparkSQL.task_63a("product", "countryip", "countryname")).collectAsList()
                .stream()
                .map(f -> String.format("%.1f %d %s", f.get(0), f.get(2), f.get(3)))
                .collect(Collectors.toList());
        List<String> expecteds = Arrays.asList("2000.0 1 United States", "1000.4 6 Norway",
                "2011.3 3 Republic of Korea");
        expecteds.stream().forEach(f -> Assert.assertTrue(actuals.contains(f)));
        Assert.assertEquals(actuals.size(), 3);
    }

}
