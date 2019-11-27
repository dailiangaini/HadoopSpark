（1）创建HBase表

    进入HBase Shell客户端执行建表命令   
    create 'hbase_test',{ NAME =>'cf'}
    
（2）插入数据
 
     执行以下命令插入数据 
     put 'hbase_test','hadoop','cf:score', '95'
     put 'hbase_test','storm','cf:score', '96'     
     put 'hbase_test','spark','cf:score', '97'
     
（3）查看数据

    执行扫描表操作
    scan 'hbase_test'
    
（4）创建Hive外部表
 
 进入Hive Shell 客户端，创建外部表course.hbase_test，建表命令如下所示：
 
 ```
create external table hbase_test(cname string,score int) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES("hbase.columns.mapping" =":key,cf:score") 
TBLPROPERTIES("hbase.table.name" ="hbase_test", "hbase.mapred.output.outputtable" = "hbase_test");
```

（5）Hive 查看数据

    执行Hive命令查询HBase 表中的数据。
    
    select * from hbase_test;