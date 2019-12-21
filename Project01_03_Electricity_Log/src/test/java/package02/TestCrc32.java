package package02;

import java.util.zip.CRC32;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/12/18 21:53
 */
public class TestCrc32 {
    public static void main(String[] args) {
        CRC32 crc32 = new CRC32();
        crc32.reset();
        crc32.update("123".getBytes());
        System.out.println(crc32.getValue());
        crc32.update("1234".getBytes());
        System.out.println(crc32.getValue());
    }
}
