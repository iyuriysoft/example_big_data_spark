package udf;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;

import schema.CountryIP;
import utils.CIDRUtils;

/**
 * It is used for register Java UDF from PySpark
 */
public class UDFGetGeoID implements UDF1<Long, Long> {
    private static final long serialVersionUID = 7157318048022803749L;

    private static Broadcast<NavigableMap<Long, Pair<Long, Long>>> broadcastMap;
    private static NavigableMap<Long, Pair<Long, Long>> map = new TreeMap<Long, Pair<Long, Long>>();

    public static void init(String name, JavaSparkContext sc) throws FileNotFoundException, IOException {
        if (!map.isEmpty())
            return;
        try (BufferedReader br = new BufferedReader(new FileReader(name));) {
            String line = br.readLine();
            while (line != null) {
                String[] ar = line.split(",");
                try {
                    Long ipstart = new CIDRUtils(ar[0]).getNetworkAddress2();
                    Long ipend = new CIDRUtils(ar[0]).getBroadcastAddress2();
                    Long geoid = Long.valueOf(ar[1]);
                    map.put(ipstart, Pair.of(ipend, geoid));

                } catch (Exception e) {
                    // TODO: handle exception
                    // skip problem values
                }
                line = br.readLine();
            }
        }
        broadcastMap = sc.broadcast(map);
    }

    public static void init(CountryIP[] countries, JavaSparkContext sc) {
        if (!map.isEmpty())
            return;
        for (CountryIP c : countries) {
            try {
                map.put(c.getStartIPAsLong(), Pair.of(c.getEndIPAsLong(), c.getGeonameId()));
            } catch (Exception e) {
                // TODO: handle exception
                // skip problem values
            }
        }
        broadcastMap = sc.broadcast(map);
    }

    public static void init(String name, SparkSession spark) throws FileNotFoundException, IOException {
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        init(name, sc);
    }
    
    public static void init(CountryIP[] countries, SparkSession spark) {
        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        init(countries, sc);
    }

    public static Long ip2geo(Long v) {
        if (map.isEmpty() || broadcastMap.value().isEmpty()) {
            throw new RuntimeException("TreeMap is empty! Before using it function init(...) should be called");
        }
        if (v == null) {
            return 0L;
        }
        Entry<Long, Pair<Long, Long>> ee = broadcastMap.value().floorEntry(v);
        if (ee == null || v > ee.getValue().getKey()) {
            return 0L;
        }
        return ee.getValue().getValue();
    }

    @Override
    public Long call(Long v) throws Exception {
        return UDFGetGeoID.ip2geo(v);
    }
}