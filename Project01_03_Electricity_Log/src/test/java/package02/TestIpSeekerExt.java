package package02;

import package02.hadoop.etl.util.IPSeekerExt;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/12/18 16:39
 */
public class TestIpSeekerExt {
    public static void main(String[] args) {
        IPSeekerExt ipSeekerExt = new IPSeekerExt();
        IPSeekerExt.RegionInfo regionInfo = ipSeekerExt.analyticIp("114.114.114.114");
        System.out.println(regionInfo);
    }
}
