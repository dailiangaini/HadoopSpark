from pyspark import SparkContext

if __name__ == '__main__':
    import os
    os.environ['PYSPARK_PYTHON'] = '/Users/dailiang/opt/anaconda3/bin/python'
    # sc = SparkContext('local', 'test')
    sc = SparkContext('spark://localhost:7077', 'test')
    # sc.setLogLevel("INFO")
    # sc = SparkContext('local', 'test')
    # textFile = sc.textFile("word.txt")  # 从HDFS上读取文件，也可以用HDFS中的详细路径"/user/hadoop/word.txt"代替"word.txt"
    # ##textFile = sc.textFile("file:///usr/local/spark/mycode/wordcount/word.txt")#或者从spark目录下直接读取

    # textFile = sc.textFile("file:///Users/dailiang/Documents/Code/StudyBigData/HadoopSpark/Project01_01_Hadoop/input/11")
    textFile = sc.textFile("/opt/test/11")
    wordCount = textFile.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(
        lambda a, b: a + b)
    list = wordCount.collect()
    # wordCount.foreach(print())
    for i in range(len(list)):
        print("序号：%s   值：%s" % (i + 1, list[i]))
