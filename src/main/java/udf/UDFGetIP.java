package udf;

import org.apache.spark.sql.api.java.UDF1;

/**
 * It is used for register Java UDF from PySpark
 */
public class UDFGetIP implements UDF1<String, Long> {
    private static final long serialVersionUID = 7157318048022803749L;

    public Long dot2LongIP(String dottedIP) {
        String[] addrArray = dottedIP.split("\\.");
        long num = 0;
        for (int i = 0; i < addrArray.length; i++) {
            int power = 3 - i;
            num += ((Integer.parseInt(addrArray[i].trim()) % 256) * Math.pow(256, power));
        }
        return num;
    }

    @Override
    public Long call(String str) throws Exception {
        if (str != null) {
            return dot2LongIP(str);
        }
        return 0L;
    }
}