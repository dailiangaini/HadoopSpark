-- 1. 在hive中创建hbase的event_logs对应表
CREATE EXTERNAL TABLE event_logs(key string, pl string, en string, s_time bigint, p_url string, u_ud string, u_sd string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties('hbase.columns.mapping'=':key,info:pl,info:en,info:s_time,info:p_url,info:u_ud,info:u_sd')
tblproperties('hbase.table.name'='event_logs');

-- 2. 创建mysql在hive中的对应表，hive中的表，执行HQL之后分析的结果保存该表，然后通过sqoop工具导出到mysql
CREATE TABLE `stats_view_depth` (`platform_dimension_id` bigint ,`data_dimension_id` bigint , `kpi_dimension_id` bigint , `pv1` bigint , `pv2` bigint , `pv3` bigint , `pv4` bigint , `pv5_10` bigint , `pv10_30` bigint , `pv30_60` bigint , `pv60_plus` bigint , `created` string);

-- 3. hive创建临时表:把hql分析之后的中间结果存放到当前的临时表。
CREATE TABLE `stats_view_depth_tmp`(`pl` string, `date` string, `col` string, `ct` bigint);

-- 4. 编写UDF(platformdimension & datedimension)<需要注意，要删除DimensionConvertClient类中所有FileSystem关闭的操作>
-- 5. 上传transformer-0.0.1.jar到hdfs的/sxt/transformer文件夹中
-- 6. 创建hive的function
drop function platform_convert;
drop function date_convert;
create function platform_convert as 'package02.hadoop.transformer.hive.PlatformDimensionUDF' using jar 'hdfs://localhost:9000/jars/HadoopSpark.jar';
create function date_convert as 'package02.hadoop.transformer.hive.DateDimensionUDF' using jar 'hdfs://localhost:9000/jars/HadoopSpark.jar';

或者
hive> add jar /home/hadoop/transformer-0.0.1.jar
hive> create temporary function lower_udf as "UDF.LowerUDF";

drop function platform_convert;
drop function date_convert;
add jar /Users/dailiang/Documents/Code/StudyBigData/HadoopSpark/out/artifacts/HadoopSpark_jar2/HadoopSpark.jar
add jar /opt/apache-hive-2.3.6/lib/mysql-connector-java-5.1.48.jar
create function platform_convert as 'package02.hadoop.transformer.hive.PlatformDimensionUDF';
create function date_convert as 'package02.hadoop.transformer.hive.DateDimensionUDF';

-- 7. hql编写(统计用户角度的浏览深度)<注意：时间为外部给定>
from (
select pl, from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as day, u_ud, (case when count(p_url) = 1 then "pv1" when count(p_url) = 2 then "pv2" when count(p_url) = 3 then "pv3" when count(p_url) = 4 then "pv4" when count(p_url) >= 5 and count(p_url) <10 then "pv5_10" when count(p_url) >= 10 and count(p_url) <30 then "pv10_30" when count(p_url) >=30 and count(p_url) <60 then "pv30_60"  else 'pv60_plus' end) as pv 
from event_logs 
where en='e_pv' and p_url is not null and pl is not null and s_time >= unix_timestamp('2016-06-08','yyyy-MM-dd')*1000 and s_time < unix_timestamp('2016-06-09','yyyy-MM-dd')*1000
group by pl, from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'), u_ud
) as tmp
insert overwrite table stats_view_depth_tmp select pl,day,pv,count(distinct u_ud) as ct where u_ud is not null group by pl,day,pv;

查询分解

    1.select pl, en, from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as day, u_ud from event_logs;

    2.select pl, from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as day, u_ud from event_logs
        where en='e_pv'
        and p_url is not null
        and pl is not null
        and s_time >= unix_timestamp('2019-12-25','yyyy-MM-dd')*1000
        and s_time < unix_timestamp('2019-12-26','yyyy-MM-dd')*1000;


        pl	    day	        u_ud
        website	2019-12-25	080c4127-2051-41fd-a989-47bd2a9834f6
        website	2019-12-25	78af68f3-e4be-43b6-85eb-0adc61a30df6
        website	2019-12-25	11b8ec7f-c4c1-4067-bcc7-71dab6485d44
        website	2019-12-25	ee96d6e0-dd91-43fa-93dc-280b570060d5

    3.select pl, from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as day, u_ud ,
          (case when count(p_url) = 1 then "pv1"
                when count(p_url) = 2 then "pv2"
                when count(p_url) = 3 then "pv3"
                when count(p_url) = 4 then "pv4"
                when count(p_url) >= 5 and count(p_url) <10 then "pv5_10"
                when count(p_url) >= 10 and count(p_url) <30 then "pv10_30"
                when count(p_url) >=30 and count(p_url) <60 then "pv30_60"  else 'pv60_plus' end) as pv
          from event_logs
          where en='e_pv'
          and p_url is not null
          and pl is not null
          and s_time >= unix_timestamp('2019-12-25','yyyy-MM-dd')*1000
          and s_time < unix_timestamp('2019-12-26','yyyy-MM-dd')*1000
          group by pl, from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'), u_ud;

    4.from (
        select pl, from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd') as day, u_ud ,
                  (case when count(p_url) = 1 then "pv1"
                        when count(p_url) = 2 then "pv2"
                        when count(p_url) = 3 then "pv3"
                        when count(p_url) = 4 then "pv4"
                        when count(p_url) >= 5 and count(p_url) <10 then "pv5_10"
                        when count(p_url) >= 10 and count(p_url) <30 then "pv10_30"
                        when count(p_url) >=30 and count(p_url) <60 then "pv30_60"  else 'pv60_plus' end) as pv
                  from event_logs
                  where en='e_pv'
                  and p_url is not null
                  and pl is not null
                  and s_time >= unix_timestamp('2019-12-25','yyyy-MM-dd')*1000
                  and s_time < unix_timestamp('2019-12-26','yyyy-MM-dd')*1000
                  group by pl, from_unixtime(cast(s_time/1000 as bigint),'yyyy-MM-dd'), u_ud
        ) as tmp
        insert overwrite table stats_view_depth_tmp select pl,day,pv,count(distinct u_ud) as ct where u_ud is not null group by pl,day,pv;

        select * from stats_view_depth_tmp;

        pl	    date	    col	   ct
        website	2019-12-25	pv1	   55
        website	2019-12-25	pv2	   36
        website	2019-12-25	pv3	   18
        website	2019-12-25	pv4	   12
        website	2019-12-25	pv5_10	3

--把临时表的多行数据，转换一行

with tmp as
(
select pl,`date` as date1,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60_plus from stats_view_depth_tmp where col='pv1' union all
select pl,`date` as date1,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60_plus from stats_view_depth_tmp where col='pv2' union all
select pl,`date` as date1,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60_plus from stats_view_depth_tmp where col='pv3' union all
select pl,`date` as date1,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60_plus from stats_view_depth_tmp where col='pv4' union all
select pl,`date` as date1,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60_plus from stats_view_depth_tmp where col='pv5_10' union all
select pl,`date` as date1,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60_plus from stats_view_depth_tmp where col='pv10_30' union all
select pl,`date` as date1,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60_plus from stats_view_depth_tmp where col='pv30_60' union all
select pl,`date` as date1,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60_plus from stats_view_depth_tmp where col='pv60_plus' union all

select 'all' as pl,`date` as date1,ct as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60_plus from stats_view_depth_tmp where col='pv1' union all
select 'all' as pl,`date` as date1,0 as pv1,ct as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60_plus from stats_view_depth_tmp where col='pv2' union all
select 'all' as pl,`date` as date1,0 as pv1,0 as pv2,ct as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60_plus from stats_view_depth_tmp where col='pv3' union all
select 'all' as pl,`date` as date1,0 as pv1,0 as pv2,0 as pv3,ct as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60_plus from stats_view_depth_tmp where col='pv4' union all
select 'all' as pl,`date` as date1,0 as pv1,0 as pv2,0 as pv3,0 as pv4,ct as pv5_10,0 as pv10_30,0 as pv30_60,0 as pv60_plus from stats_view_depth_tmp where col='pv5_10' union all
select 'all' as pl,`date` as date1,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,ct as pv10_30,0 as pv30_60,0 as pv60_plus from stats_view_depth_tmp where col='pv10_30' union all
select 'all' as pl,`date` as date1,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,ct as pv30_60,0 as pv60_plus from stats_view_depth_tmp where col='pv30_60' union all
select 'all' as pl,`date` as date1,0 as pv1,0 as pv2,0 as pv3,0 as pv4,0 as pv5_10,0 as pv10_30,0 as pv30_60,ct as pv60_plus from stats_view_depth_tmp where col='pv60_plus'
)
from tmp
insert overwrite table stats_view_depth
    select platform_convert(pl),date_convert(date1),5,
        sum(pv1),sum(pv2),sum(pv3),sum(pv4),sum(pv5_10),sum(pv10_30),
        sum(pv30_60),sum(pv60_plus),'2019-12-25'
    group by pl,date1;



platform_convert(pl),date_convert(date1)

`platform_dimension_id` bigint ,
    1	all
    2	website
`data_dimension_id` bigint ,
    1	2019	4	12	52	25	2019-12-25	day
`kpi_dimension_id` bigint ,


select platform_dimension_name, data_dimension_name,kpi_dimension_id , pv1, pv2, pv3, pv4, pv5_10, pv10_30, pv30_60, pv60_plus,`created` from stats_view_depth2;


select platform_convert(platform_dimension_name), date_convert(data_dimension_name),kpi_dimension_id , pv1, pv2, pv3, pv4, pv5_10, pv10_30, pv30_60, pv60_plus from stats_view_depth2;

select * from stats_view_depth;

-- 9. sqoop脚步编写(统计会话角度)
sqoop export --connect jdbc:mysql://localhost:3306/report --username root --password 123456 --table stats_view_depth --export-dir /hive/bigdater.db/stats_view_depth/* --input-fields-terminated-by "\\01" --update-mode allowinsert --update-key platform_dimension_id,data_dimension_id,kpi_dimension_id

-- 10. shell脚步编写
