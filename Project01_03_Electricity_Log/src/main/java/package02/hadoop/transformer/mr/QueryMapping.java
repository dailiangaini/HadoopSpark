package package02.hadoop.transformer.mr;

import package02.hadoop.common.KpiType;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/12/25 00:42
 */
public class QueryMapping {

    public static String getSql(KpiType kpiType){
        switch (kpiType){
            // 新用户
            case NEW_INSTALL_USER:{
                return "INSERT INTO `stats_user`(" +
                        "`platform_dimension_id`," +
                        "`date_dimension_id`," +
                        "`new_install_users`," +
                        "`created`)" +
                        "VALUES(?, ?, ?, ?) ON DUPLICATE KEY UPDATE `new_install_users` = ?";
            }
            // 浏览器新用户
            case BROWSER_NEW_INSTALL_USER:{
                return "INSERT INTO `stats_device_browser`(" +
                        "    `platform_dimension_id`," +
                        "    `date_dimension_id`," +
                        "    `browser_dimension_id`," +
                        "    `new_install_users`," +
                        "    `created`)" +
                        "  VALUES(?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `new_install_users` = ?";
            }
            case ACTIVE_USER:{
                return "INSERT INTO `stats_user`(" +
                        "    `platform_dimension_id`," +
                        "    `date_dimension_id`," +
                        "    `active_users`," +
                        "    `created`)" +
                        "  VALUES(?, ?, ?, ?) ON DUPLICATE KEY UPDATE `active_users` = ?";
            }
            case BROWSER_ACTIVE_USER:{
                return "INSERT INTO `stats_device_browser`(" +
                        "    `platform_dimension_id`," +
                        "    `date_dimension_id`," +
                        "    `browser_dimension_id`," +
                        "    `active_users`," +
                        "    `created`" +
                        "  ) VALUES(?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `active_users` = ?";
            }
            default: {
                return null;
            }
        }
    }
}
