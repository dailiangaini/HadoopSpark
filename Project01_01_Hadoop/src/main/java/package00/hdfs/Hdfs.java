package package00.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.common.JspHelper;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.mortbay.util.Scanner;

import java.io.*;
import java.net.URI;
/**
 * @Author: D&L
 * @Description:
 * @Date: 2019/11/26 09:20
 */
public class Hdfs {
    private static Configuration conf = new Configuration();



    public static void main(String[] args) throws Exception{
        URI uri = new URI("hdfs://localhost:9000");
         mkdirs(uri);
         createNewFile(uri);
         addSomeThings(uri);
         appendSomeThings(uri);
        readFile(uri);

    }

    //创建一个文件夹
    private static void mkdirs(URI uri) throws Exception{
        FileSystem fs = FileSystem.get(uri,conf);
        boolean mkdirs = fs.mkdirs(new Path("/abba"));
        if(mkdirs){
            System.out.println("ok");
        }else {
            System.out.println("?");
        }
    }

    //在上一个创建的文件夹内创建一个文件
    private static void createNewFile(URI uri) throws Exception{
        FileSystem fs = FileSystem.get(uri,conf);
        boolean b = fs.createNewFile(new Path("/abba/a.txt"));
        if(b){
            System.out.println("ok");
        }else {
            System.out.println("?");
        }
    }

    //文件添加内容
    private static void addSomeThings(URI uri) throws Exception {

        FileSystem fs = FileSystem.get(uri,conf);
        FSDataOutputStream append = fs.append(new Path("/abba/a.txt"));
        BufferedWriter bf =new BufferedWriter(new OutputStreamWriter(append));

        bf.write("  这一行没换行");
        //换行
        bf.newLine();
        bf.write("这一行换行");

        bf.close();
        fs.close();
    }

    //文件内容追加
    private static void appendSomeThings(URI uri) throws Exception {

        FileSystem fs = FileSystem.get(uri,conf);
        FSDataOutputStream append = fs.append(new Path("/abba/a.txt"));
        append.write("hello world ！".getBytes());
        append.close();
        fs.close();
    }

    //读取文件内容
    private static void readFile(URI uri) throws Exception {

        FileSystem fs = FileSystem.get(uri,conf);
        InputStream is = fs.open(new Path("/abba/a.txt"));
        BufferedReader bf = new BufferedReader(new InputStreamReader(is));

        String s = null;
        while ((s = bf.readLine()) != null){
            System.err.println(s);
        }

        is.close();
        bf.close();
        fs.close();
    }
}
