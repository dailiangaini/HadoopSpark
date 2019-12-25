package package02.hadoop.transformer.mr;

import package02.hadoop.common.KpiType;
import package02.hadoop.transformer.mr.activeuser.ActiveUserBrowserCollector;
import package02.hadoop.transformer.mr.activeuser.ActiveUserCollector;
import package02.hadoop.transformer.mr.newuser.StatsBrowserUserNewInstallUserCollector;
import package02.hadoop.transformer.mr.newuser.StatsUserNewInstallUserCollector;

/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/12/25 00:33
 */
public class OutputCollectorFactory {
    private static IOutputCollector newUserOutputCollector;
    private static IOutputCollector browserNewUserOutputCollector;
    private static IOutputCollector activeUserOutputCollector;
    private static IOutputCollector browserActiveUserOutputCollector;

    public static IOutputCollector getOutputCollector(KpiType kpiType){
        switch (kpiType){
            // 新用户
            case NEW_INSTALL_USER:{
                if(null == newUserOutputCollector){
                    newUserOutputCollector = new StatsUserNewInstallUserCollector();
                }
                return newUserOutputCollector;
            }
            // 浏览器新用户
            case BROWSER_NEW_INSTALL_USER:{
                if(null == browserNewUserOutputCollector){
                    browserNewUserOutputCollector = new StatsBrowserUserNewInstallUserCollector();
                }
                return browserNewUserOutputCollector;
            }
            case ACTIVE_USER:{
                if(null == activeUserOutputCollector){
                    activeUserOutputCollector = new ActiveUserCollector();
                }
                return activeUserOutputCollector;
            }
            case BROWSER_ACTIVE_USER:{
                if(null == browserActiveUserOutputCollector){
                    browserActiveUserOutputCollector = new ActiveUserBrowserCollector();
                }
                return browserActiveUserOutputCollector;
            }
            default: {
                return null;
            }
        }
    }
}
