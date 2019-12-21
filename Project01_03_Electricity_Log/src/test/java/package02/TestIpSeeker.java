package package02;

import package02.hadoop.etl.util.ip.IPSeeker;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/12/18 16:39
 */
public class TestIpSeeker {
    public static void main(String[] args) {
        IPSeeker ipSeeker = IPSeeker.getInstance();
        System.out.println(ipSeeker.getCountry("120.197.87.216"));
        System.out.println(ipSeeker.getCountry("192.168.1.14"));
    }
}
