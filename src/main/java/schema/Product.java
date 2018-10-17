package schema;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Product implements Serializable {
    private static final long serialVersionUID = -1609125013029216468L;
    private SimpleDateFormat sdfDateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private String name;
    private String price;
    private String dt;
    private String category;
    private String ip;

    public Product(String str) {
        this(str.split(","));
    }
    
    public Product(String[] fields) {
        this.name = fields[0].trim();
        this.price = fields[1].trim();
        this.dt = fields[2].trim();
        this.category = fields[3].trim();
        this.ip = fields[4].trim();
    }

    private Long dot2LongIP(String dottedIP) {
        String[] addrArray = dottedIP.split("\\.");
        long num = 0;
        for (int i = 0; i < addrArray.length; i++) {
            int power = 3 - i;
            num += ((Integer.parseInt(addrArray[i].trim()) % 256) * Math.pow(256, power));
        }
        return num;
    }

    public String getPrice() {
        return price;
    }

    public Float getPriceAsFloat() {
        return Float.parseFloat(price);
    }

    public String getName() {
        return name;
    }

    public String getCategory() {
        return category;
    }

    public long getDateAsLong() throws ParseException {
        return sdfDateTime.parse(dt).getTime();
    }

    public String getIP() {
        return ip;
    }

    public Long getIPAsLong() {
        return dot2LongIP(ip);
    }

    public String getDate() {
        return dt;
    }
}