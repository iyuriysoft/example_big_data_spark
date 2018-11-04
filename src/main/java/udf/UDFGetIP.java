package udf;

import org.apache.spark.sql.api.java.UDF1;

import utils.CIDRUtils;

/**
 * It is used for register Java UDF from PySpark
 */
public class UDFGetIP implements UDF1<String, Long> {
    private static final long serialVersionUID = 7157318048022803749L;

    @Override
    public Long call(String str) throws Exception {
        if (str != null) {
            return CIDRUtils.dot2LongIP(str);
        }
        return 0L;
    }
}