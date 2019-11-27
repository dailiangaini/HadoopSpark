hbase shell进入命令行

0.查看所有表
    
    可用list 命令查看当前创建的表：
    hbase(main):026:0> list
    TABLE                                                                                                                                                                                          
    ai_ns:testtable           

1.创表

    create 'table', 'column_family_1','column_family_2','column_family_3'...

    hbase(main):032:0> create 'hbase_test','column_family_1', 'column_family_2', 'column_family_3'
    0 row(s) in 1.2620 seconds
    
    => Hbase::Table - hbase_test
    hbase(main):033:0> 
    
2.查看表结构

    可使用describe命令查看表结构，其规范为：
    describe 'table'
    
    hbase(main):033:0> describe 'hbase_test'
    Table hbase_test is ENABLED                                                                                                                                                                    
    hbase_test                                                                                                                                                                                     
    COLUMN FAMILIES DESCRIPTION                                                                                                                                                                    
    {NAME => 'column_family_1', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE',
     MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}                                                                                                    
    {NAME => 'column_family_2', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE',
     MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}                                                                                                    
    {NAME => 'column_family_3', BLOOMFILTER => 'ROW', VERSIONS => '1', IN_MEMORY => 'false', KEEP_DELETED_CELLS => 'FALSE', DATA_BLOCK_ENCODING => 'NONE', TTL => 'FOREVER', COMPRESSION => 'NONE',
     MIN_VERSIONS => '0', BLOCKCACHE => 'true', BLOCKSIZE => '65536', REPLICATION_SCOPE => '0'}                                                                                                    
    3 row(s) in 0.0290 seconds
    
3.插入与更新数据
    
    如何插入数据呢？较为普遍的方法是put命令，其命令规范为：
    put 'table', 'row key', 'column_family:column', 'value'
    
    hbase(main):007:0> put 'hbase_test','key_1','column_family_1:column_1','value1'
    0 row(s) in 0.1710 seconds
    
    hbase(main):008:0> put 'hbase_test','key_1','column_family_1:column_2','value2'
    0 row(s) in 0.0150 seconds
 
    put 'hbase_test','key_2','column_family_1:column_1','value4'
    put 'hbase_test','key_2','column_family_1:column_2','value5'
    put 'hbase_test','key_3','column_family_1:column_2','value5'
    put 'hbase_test','key_4','column_family_1:column_1','value1'
    put 'hbase_test','key_4','column_family_2:column_3','value3'
    put 'hbase_test','key_4','column_family_3:','value1'
    put 'hbase_test','key_4','column_family_3:','value1'
    put 'hbase_test','key_5','column_family_1:column_1','value1'
    put 'hbase_test','key_5','column_family_2:column_3','value4'
    put 'hbase_test','key_5','column_family_3:','value2'

4.读取数据（get）

    以上这样的插入数据方式，很容易联想到获取数据是否也是类似格式呢？
    没错，获取数据get 命令与之相似：
    hbase(main):020:0> get 'hbase_test','key_1' 
    COLUMN                                           CELL                                                                                                                                          
     column_family_1:column_1                        timestamp=1534259899359, value=value1                                                                                                         
     column_family_1:column_2                        timestamp=1534259904389, value=value2                                                                                                         
    2 row(s) in 0.0220 seconds
    
    get 't1', 'r1', {COLUMN => 'c1', TIMERANGE => [ts1, ts2], VERSIONS => 4}

5.读取数据（scan）    
    
    使用get命令虽然方便，但是终究只是某一个row key下的数据，若需要查看所有数据，明显不能满足我们工作需求。
    别急，还可以使用scan命令查看数据
    
    hbase(main):021:0> scan 'hbase_test'
     ROW                                              COLUMN+CELL                                                                                                                                   
     key_1                                           column=column_family_1:column_1, timestamp=1534259899359, value=value1                                                                        
     key_1                                           column=column_family_1:column_2, timestamp=1534259904389, value=value2                                                                        
     key_2                                           column=column_family_1:column_1, timestamp=1534259909024, value=value4                                                                        
     key_2                                           column=column_family_1:column_2, timestamp=1534259913358, value=value5                                                                        
     key_3                                           column=column_family_1:column_2, timestamp=1534259917322, value=value5                                                                        
     key_4                                           column=column_family_1:column_1, timestamp=1534259924209, value=value1                                                                        
     key_4                                           column=column_family_2:column_3, timestamp=1534259928680, value=value3                                                                        
     key_4                                           column=column_family_3:, timestamp=1534259936240, value=value1                                                                                
     key_5                                           column=column_family_1:column_1, timestamp=1534259939697, value=value1                                                                        
     key_5                                           column=column_family_2:column_3, timestamp=1534259943330, value=value4                                                                        
     key_5                                           column=column_family_3:, timestamp=1534259947358, value=value2                                                                                
    5 row(s) in 0.0420 seconds
    
    以上为获取某张表的所有数据，若只需取’column_family_1’列族下的数据，则：
    hbase(main):002:0> scan 'hbase_test',{COLUMN => 'column_family_1'}
    上述表达式 等同于 scan 'hbase_test',{COLUMN => ['column_family_1']}
    hbase(main):004:0> scan 'hbase_test',{COLUMN => ['column_family_1', 'column_family_2']}
    hbase(main):005:0> scan 'hbase_test',{COLUMN => ['column_family_1:column_1']}
    hbase(main):006:0> scan 'hbase_test',{COLUMN => ['column_family_1:column_1','column_family_2:column_3']}
    
    hbase(main):007:0> scan 'hbase_test',{COLUMN => ['column_family_1:column_1','column_family_2:column_3'], STARTROW => 'key_2'}

    hbase(main):009:0> scan 'hbase_test',{COLUMN => ['column_family_1:column_1','column_family_2:column_3'], STARTROW => 'key_2', STOPROW => 'key_5'}

    使用scan 命令扫描一张表的数据时，我们常常会限制下输出row key条数：
    hbase(main):013:0> scan 'hbase_test', {LIMIT => 2}
    若想以反序获取两行数据：
    hbase(main):016:0> scan 'hbase_test', {LIMIT => 2, REVERSED => true}
   
6.删除数据（delete）

    难免有数据插入不当的情况，可用delete命令删除：
    delete 'hbase_test','key_6','column_family_3:'

7.删除数据（deleteall）

    delete这个方法只能删除具体到哪一行中的某个列族下的某一列数据，想要删除一整行数据，需用deleteall命令：
    deleteall 'hbase_test','key_7'
    
8.truncate

    若需删除整张表的数据，可用truncate命令：
    hbase(main):050:0> truncate 'hbase_test'    

9.删除表
    
    应的，删除表可用drop命令：
    表创建成功后，默认状态是enable，即“使用中”的状态，删除表之前需先设置表为“关闭中”。
    设置表为“使用中”：enable 'hbase_test'
    设置表为“关闭中”：disable 'hbase_test'
    
    disable 'hbase_test'
    drop 'hbase_test'
    
10.执行脚本
    
    hbase 与 hive一样，都是可以直接执行脚本的。比如之前的put 命令，一个个填写复制粘贴写数据很麻烦，我可以全部put 命令放在一个文件中：
    
    touch hbase_test
    hbase shell hbase_test
    
