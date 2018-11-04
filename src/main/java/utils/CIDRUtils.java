package utils;

import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A class that enables to get an IP range from CIDR specification. It supports
 * both IPv4 and IPv6.
 */
public class CIDRUtils {
    private final String cidr;

    private InetAddress inetAddress;
    private InetAddress startAddress;
    private InetAddress endAddress;
    private final int prefixLength;
    private Long istartAddress;
    private Long iendAddress;

    public CIDRUtils(String cidr) throws UnknownHostException {

        this.cidr = cidr;

        /* split CIDR to address and prefix part */
        if (this.cidr.contains("/")) {
            int index = this.cidr.indexOf("/");
            String addressPart = this.cidr.substring(0, index);
            String networkPart = this.cidr.substring(index + 1);

            inetAddress = InetAddress.getByName(addressPart);
            prefixLength = Integer.parseInt(networkPart);

            calculate();
        } else {
            throw new IllegalArgumentException("not an valid CIDR format!");
        }
    }

    private static final Pattern PATTERN = Pattern.compile(
            "^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$");

    public static boolean validate(final String ip) {
        return PATTERN.matcher(ip).matches();
    }

    public static Long dot2LongIP(String dottedIP) {
        // A 10.0.0.0 255.0.0.0 10.0.0.0 - 10.255.255.255
        // B 172.16.0.0 - 172.31.0.0 255.240.0.0 172.16.0.0 - 172.31.255.255
        // C 192.168.0.0 255.255.0.0 192.168.0.0 - 192.168.255.255
        //if (!validate(dottedIP)) {
        //    return null;
        //}
        //if (dottedIP.startsWith("10.") || dottedIP.startsWith("192.168.")) {
        //    return null;
        //}
        String[] addrArray = dottedIP.split("\\.");
        //if (Integer.valueOf(addrArray[0]) == 172
        //        && Integer.valueOf(addrArray[1]) >= 16
        //        && Integer.valueOf(addrArray[1]) <= 31) {
        //    return null;
        //}
        long num = 0;
        for (int i = 0; i < addrArray.length; i++) {
            int power = 3 - i;
            num += ((Integer.parseInt(addrArray[i].trim()) % 256) * Math.pow(256, power));
        }
        return num;
    }

    private void calculate() throws UnknownHostException {
        ByteBuffer maskBuffer;
        int targetSize;
        if (inetAddress.getAddress().length == 4) {
            maskBuffer = ByteBuffer.allocate(4).putInt(-1);
            targetSize = 4;
        } else {
            maskBuffer = ByteBuffer.allocate(16).putLong(-1L).putLong(-1L);
            targetSize = 16;
        }

        BigInteger mask = (new BigInteger(1, maskBuffer.array())).not().shiftRight(prefixLength);

        ByteBuffer buffer = ByteBuffer.wrap(inetAddress.getAddress());
        BigInteger ipVal = new BigInteger(1, buffer.array());

        BigInteger startIp = ipVal.and(mask);
        BigInteger endIp = startIp.add(mask.not());
        istartAddress = startIp.longValue();
        iendAddress = endIp.longValue();

        byte[] startIpArr = toBytes(startIp.toByteArray(), targetSize);
        byte[] endIpArr = toBytes(endIp.toByteArray(), targetSize);

        this.startAddress = InetAddress.getByAddress(startIpArr);
        this.endAddress = InetAddress.getByAddress(endIpArr);

    }

    private byte[] toBytes(byte[] array, int targetSize) {
        int counter = 0;
        List<Byte> newArr = new ArrayList<Byte>();
        while (counter < targetSize && (array.length - 1 - counter >= 0)) {
            newArr.add(0, array[array.length - 1 - counter]);
            counter++;
        }

        int size = newArr.size();
        for (int i = 0; i < (targetSize - size); i++) {
            newArr.add(0, (byte) 0);
        }

        byte[] ret = new byte[newArr.size()];
        for (int i = 0; i < newArr.size(); i++) {
            ret[i] = newArr.get(i);
        }
        return ret;
    }

    public String getNetworkAddress() {
        return this.startAddress.getHostAddress();
    }

    public String getBroadcastAddress() {
        return this.endAddress.getHostAddress();
    }

    public Long getNetworkAddress2() {
        return this.istartAddress;
    }

    public Long getBroadcastAddress2() {
        return this.iendAddress;
    }

    public boolean isInRange(String ipAddress) throws UnknownHostException {
        InetAddress address = InetAddress.getByName(ipAddress);
        BigInteger start = new BigInteger(1, this.startAddress.getAddress());
        BigInteger end = new BigInteger(1, this.endAddress.getAddress());
        BigInteger target = new BigInteger(1, address.getAddress());

        int st = start.compareTo(target);
        int te = target.compareTo(end);

        return (st == -1 || st == 0) && (te == -1 || te == 0);
    }
}