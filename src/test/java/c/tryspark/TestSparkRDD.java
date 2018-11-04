package c.tryspark;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import scala.Tuple2;
import schema.CountryIP;
import schema.CountryName;
import schema.Product;
import tryspark.SparkRDD;
import udf.UDFGetGeoID;

/**
 * Unit test for Spark RDD.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestSparkRDD {

    private static JavaSparkContext sparkCtx;
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
        SparkConf conf = new SparkConf();
        conf.setMaster("local[*]");
        conf.setAppName("junit");
        sparkCtx = new JavaSparkContext(conf);
    }
    
    @AfterClass
    public static void exit() {
        sparkCtx.close();
    }

    @Test
    public void test_Task51_1() {
        JavaRDD<Product> inputRDD = sparkCtx.parallelize(Arrays.asList(arInput));
        JavaPairRDD<Integer, String> result = SparkRDD.task_51_approach_1(inputRDD);
        
        List<String> actuals = result.map(f -> String.format("%d %s", f._1, f._2)).collect();
        List<String> expecteds = Arrays.asList("4 category0", "4 category1", "1 category2", "1 category3");
        expecteds.stream().forEach(f -> Assert.assertTrue(actuals.contains(f)));
        Assert.assertEquals(actuals.size(), 4);
    }

    @Test
    public void test_Task52_1() {
        JavaRDD<Product> inputRDD = sparkCtx.parallelize(Arrays.asList(arInput));
        JavaPairRDD<Integer, Tuple2<String, String>> result = SparkRDD.task_52_approach_1(inputRDD);
        
        List<String> actuals = result.map(f -> String.format("%d %s %s", f._1, f._2._1, f._2._2)).collect();
        List<String> expecteds = Arrays.asList("4 category0 product2", "3 category1 product0", "1 category1 product2",
                "1 category3 product4", "1 category2 product1");
        expecteds.stream().forEach(f -> Assert.assertTrue(actuals.contains(f)));
        Assert.assertEquals(actuals.size(), 5);
        // actuals.stream().forEach(f -> System.out.println(f));
    }

    @Test
    public void test_Task63_1() {
        JavaRDD<CountryName> rddCountryName = sparkCtx.parallelize(Arrays.asList(arCountryName));
        JavaRDD<Product> inputRDD = sparkCtx.parallelize(Arrays.asList(arInput));

        JavaPairRDD<Long, CountryName> rddName = rddCountryName.mapToPair(f -> new Tuple2<>(f.getGeonameId(), f));

        UDFGetGeoID.init(arCountryIP, sparkCtx);
        JavaPairRDD<Float, Tuple2<Long, String>> result = SparkRDD
                .task_63_approach_1(inputRDD, rddName, sparkCtx);

        List<String> actuals = result.map(f -> String.format("%.1f %d %s", f._1, f._2._1, f._2._2)).collect();
        List<String> expecteds = Arrays.asList("2000.0 1 United States", "1000.4 6 Norway",
                "2011.3 3 Republic of Korea");
        expecteds.stream().forEach(f -> Assert.assertTrue(actuals.contains(f)));
        Assert.assertEquals(actuals.size(), 3);
    }

    @Test
    public void test_Task63_2() {
        JavaRDD<CountryName> rddCountryName = sparkCtx.parallelize(Arrays.asList(arCountryName));
        JavaRDD<CountryIP> rddCountryIP = sparkCtx.parallelize(Arrays.asList(arCountryIP));
        JavaRDD<Product> inputRDD = sparkCtx.parallelize(Arrays.asList(arInput));

        JavaPairRDD<Long, CountryIP> aIP = rddCountryIP.mapToPair(f -> new Tuple2<>(f.getGeonameId(), f));
        JavaPairRDD<Long, CountryName> aName = rddCountryName.mapToPair(f -> new Tuple2<>(f.getGeonameId(), f));
        JavaPairRDD<Long, Tuple2<CountryIP, CountryName>> rddCountryNameIP = aIP.join(aName);

        JavaPairRDD<Float, Tuple2<Long, String>> result = SparkRDD
                .task_63_approach_2(inputRDD, rddCountryNameIP, sparkCtx);

        List<String> actuals = result.map(f -> String.format("%.1f %d %s", f._1, f._2._1, f._2._2)).collect();
        List<String> expecteds = Arrays.asList("2000.0 1 United States", "1000.4 6 Norway",
                "2011.3 3 Republic of Korea");
        expecteds.stream().forEach(f -> Assert.assertTrue(actuals.contains(f)));
        Assert.assertEquals(actuals.size(), 3);
    }

}
