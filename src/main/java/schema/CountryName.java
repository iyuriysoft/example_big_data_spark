package schema;

import java.io.Serializable;

/**
 * 
 * schemaName = StructType([ StructField("geoname_id", LongType()),
 * StructField("locale_code", StringType()), StructField("continent_code",
 * StringType()), StructField("continent_name", StringType()),
 * StructField("country_iso_code", StringType()), StructField("country_name",
 * StringType()), StructField("is_in_e", LongType()), ])
 */
public class CountryName implements Serializable {
    private static final long serialVersionUID = -1609125013029216468L;
    private String country_name;
    private Long geoname_id;

    public CountryName(String str) {
        this(str.split(","));
    }

    public CountryName(String[] fields) {
        this.geoname_id = fields[0].trim().isEmpty() ? 0L : Long.valueOf(fields[0].trim());
        this.country_name = fields[5].trim();
    }

    public String getCountryName() {
        return country_name;
    }

    public Long getGeonameId() {
        return geoname_id;
    }

}