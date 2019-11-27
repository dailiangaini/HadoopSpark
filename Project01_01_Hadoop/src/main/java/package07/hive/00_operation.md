#### Hive之示例：基本操作与案例
##### 1. 创建数据库，切换数据库
```    
create database testdb;
use testdb;
```
##### 2. 创建管理表
```
create table emp(
    empno int,
    empname string,
    job string,
    mgr int,
    hiredate string,
    salary double,
    comm double,
    deptno int)
    row format delimited
    fields terminated by '\t';
 
加载数据
load data local inpath '/opt/test/emp.txt' overwrite into table emp; 
 ```
emp.txt文件内容如下：
```
101	'duan'	'it'	1	'hiredate'	100.0	10.0	1
102	'duan2'	'product'	1	'2018'	200.0	20.0	1   
```
##### 3. 创建外部表 
创建外部表时直接指定表位置

上传数据文件到指定路径  
```
 hdfs dfs -mkdir /hive/warehouse/testdb.db/emp_ext
 hdfs dfs -put emp.txt /hive/warehouse/testdb.db/emp_ext/
```
在hive中创建数据表指定location
```
create external table emp_ext(
    empno int,
    empname string,
    job string,
    mgr int,
    hiredate string,
    salary double,
    comm double,
    deptno int)
row format delimited
fields terminated by '\t'
location '/hive/warehouse/testdb.db/emp_ext/';
```
##### 4. 创建分区表
```
create table emp_part(
    empno int,
    empname string,
    job string,
    mgr int,
    hiredate string,
    salary double,
    comm double,
    deptno int)
partitioned by (year string, month string)
row format delimited
fields terminated by '\t';
```
注：分区字段不能与表中其他字段重复，否则报错 
FAILED: SemanticException [Error 10035]: Column repeated in partitioning columns 

###### 加载数据 
###### 1、将txt的文本文件导入hive
从本地拷贝emp.txt到分区表目录中

```
load data local inpath '/home/duanxz/hive/hivelocal/emp.txt' into table emp_part partition (year='2018', month='5');
load data local inpath '/home/duanxz/hive/hivelocal/emp2.txt' into table emp_part partition (year='2018', month='6'); 
```
用hdfs中指定位置的数据，增加分区表中数据，此操作不会移动数据文件到分区表目录中

```
alter table emp_part add partition (year='2016', month='5') location '/data'; 
```
把hdfs中指定位置的数据移动到分区表目录中，增加数据

```
load data inpath '/emp.txt' into table emp_part partition (year='2016', month='6');
```
###### 2、将csv导入hive

```
create table feizhou_china_part2(
    merchant string,
    pay_time string,
    currency string,
    amount double,
    fee double,
    transaction_reference string,
    feizhou_reference string,
    link_reference string,
    narration string,
    account_number string,
    account_name string,
    bank string,
    bank_code string,
    status string,
    source string)
partitioned by (year string, month string, day string)
row format delimited
fields terminated by '?';
```
导入：

```
load data local inpath '/home/duanxz/hive/hivelocal/china-pay-disburse-transactions.csv' into table feizhou_china_part2 partition (year='2018',month='06',day='19');
```
说明：上面的为什么将分隔符调整为"?"呢，是因为csv中默认的分隔符是','，内容中如果有','，这样导入后，内容就乱了。

[修改CSV文件的分隔符](https://www.cnblogs.com/duanxz/p/9200751.html)

##### 5.其他创建表的方式 
(1) create-as

```
create table emp3  
as
select * from emp; 
```
(2) create-like

```
create table emp4 like emp;
load data local inpath '/opt/test/emp.txt' overwrite into table emp4;
```
(3)插入数据
```
insert overwrite table emp4 select * from emp;
```
##### 6.指定表存储格式与压缩格式 

```
create table emp_orc(
    empno int,
    empname string,
    job string,
    mgr int,
    hiredate string,
    salary double,
    comm double,
    deptno int)
stored as orc;
```
##### 7.hive执行参数-e,-f,--hiveconf 
(1)命令行直接执行hql语句
```
hive -e "select * from db_hive01.emp"
```
(2)执行hql文件中的语句
```
hive -f emp.hql
```
(3)打开调试模式
```
hive --hiveconf hive.root.logger=DEBUG,console
```

##### 8.数据导出 
(1)导出数据到本地 

a)insert


```
insert overwrite local directory '/opt/test/local'  
row format delimited fields terminated by '\t'
select * from emp; 
```
**如果不指定row format delimited fields terminated by '\t'，字段间默认没有分割符**
b)
```
hive -e 'select * from testdb2.emp'  >> ./emp_export.txt
```

 
(2)导出到hdfs 

a)
```
insert overwrite directory '/export_data'  
select * from emp;
```

hive 0.13.1版本还不支持导出数据到hdfs时指定分隔符row format delimited fields terminated by '\t' 
 
b)

```
export table emp to '/export_data';
```

导出后会在会生成/export_data/data目录， emp.txt存放在此目录中，即/export_data/data/emp.txt 
 

##### 9. 排序 
(1)order by 全局排序

```
insert overwrite local directory '/opt/test/local'  
row format delimited fields terminated by '\t'
select * from emp order by empno;
```

(2)sort by 与 distributed by 

类似MR中partition，进行分区，结合sort by使用 
每个reduce内部进行排序，全局不是排序， distribute by 一定是放在sort by 前面， 
且必须要指定mapreduce.job.reduces数量，否则导出结果还是在一个文件中

```
set mapreduce.job.reduces=3;
insert overwrite local directory '/opt/test/local'  
row format delimited fields terminated by '\t'
select * from emp distribute by deptno sort by empno;
```

(3)cluster by

当distributed by和sort by 字段一样的时候，直接使用cluster by 
 

##### 10.常用函数
```
select upper(empname) from emp;
select unix_timestamp(trackTime) from bflog limit 3 ;
select year(hiredate) from emp ;
select month(hiredate) from emp ;
select hour(hiredate) from emp ;
select substr(hiredate,1,4) from .emp ;
select split(hiredate,'-')[1] from emp ;
select reverse(hiredate) from emp ;
select concat(empno,'-',empname) from emp ;

case when 条件1  then  ...
     when 条件2  then  ...
     else  end  
```

##### 11. 自定义UDF

```
add jar /opt/test/mylower.jar ;
CREATE TEMPORARY FUNCTION mylower AS 'org.gh.hadoop.hive.MyLower';
```

##### 12. 使用正则表达式加载数据字段

```
create table beifenglog(
    remote_addr string,
    remote_user string,
    time_local string,
    request string,
    status string,
    body_bytes_sent string,
    request_body string,
    http_referer string,
    http_user_agent string,
    http_x_forwarded_for string,
    host string)
    row format serde 'org.apache.hadoop.hive.contrib.serde2.RegexSerDe'
    with serdeproperties(
    "input.regex" = "(\\\"[\\d\\.]+\\\") (\\\"[^ ]+\\\") (\\\".*?\\\") (\\\".*?\\\") (\\\"\\d+\\\") (\\\"\\d+\\\") ([^ ]+) (\\\"[^ ]+\\\") (\\\".*?\\\") (\\\"[^ ]+\\\") (\\\"[^ ]+\\\")"
)
stored as textfile;
 
加载原表数据
load data local inpath '/opt/test/beifenglog.data' overwrite into table beifenglog;
```
可以使用工具调试正则：http://tool.chinaz.com/regex 

##### 13.注意点 
(1)在创建表（无论管理表还是外部表）时，如果没有指定location，可以使用load data加载数据

a) 指定本地目录中的数据，会上传数据文件到hdfs中

b) 指定hdfs中数据文件，如果指定的路径与表所在的目录不一致，则移动数据文件到表目录中 

```
create external table emp_ext2 like emp;
load data inpath '/emp.txt' into table emp_ext2;
会把/emp.txt移动到/user/hive/warehouse/testdb2.db/emp_ext2/目录中 
create table emp2 like emp;
load data inpath '/emp.txt' into table emp2;
会把/emp.txt移动到/user/hive/warehouse/testdb2.db/emp2/目录中
```
(2)create-like时不能指定stored as为其他格式，否则报错 
以下操作会报错 FAILED: ParseException line 1:31 missing EOF at 'stored' near 'emp'

```
create table emp_orc2 like emp stored as orc;
```









