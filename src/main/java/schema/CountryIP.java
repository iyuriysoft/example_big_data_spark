package schema;

import java.io.Serializable;
import java.net.UnknownHostException;

import utils.CIDRUtils;

/**
 * schemaIP = StructType([ StructField("network", StringType()),
 * StructField("geoname_id", LongType()), StructField("registered_id",
 * LongType()), StructField("represented_id", LongType()),
 * StructField("is_anonymous", LongType()), StructField("is_satellite",
 * LongType()), ])
 *
 */
public class CountryIP implements Serializable {
    private static final long serialVersionUID = -1609125013029216468L;
    private String network;
    private Long geoname_id;

    public CountryIP(String[] fields) {
        this.network = fields[0].trim();
        this.geoname_id = fields[1].trim().isEmpty() ? 0L : Long.valueOf(fields[1].trim());
    }

    public String getNetwork() {
        return network;
    }

    public Long getStartIPAsLong() throws UnknownHostException {
        return new CIDRUtils(network).getNetworkAddress2();
    }

    public Long getEndIPAsLong() throws UnknownHostException {
        return new CIDRUtils(network).getBroadcastAddress2();
    }

    public Long getGeonameId() {
        return geoname_id;
    }

}