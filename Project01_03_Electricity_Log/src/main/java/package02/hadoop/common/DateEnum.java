package package02.hadoop.common;

/**
 * 日期类型枚举类
 * 
 * @author root
 *
 */
public enum DateEnum {
    /**
     * 时间枚举
     * YEAR：年
     * SEASON：季
     * MONTH：月
     * WEEK：周
     * DAY：天
     * HOUR：小时
     */
    YEAR("year"),
    SEASON("season"),
    MONTH("month"),
    WEEK("week"),
    DAY("day"),
    HOUR("hour");

    public final String name;

    private DateEnum(String name) {
        this.name = name;
    }
    /**
     * 根据属性name的值获取对应的type对象
     * 
     * @param name
     * @return
     */
    public static DateEnum valueOfName(String name) {
        for (DateEnum type : values()) {
            if (type.name.equals(name)) {
                return type;
            }
        }
        return null;
    }
}
