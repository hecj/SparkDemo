package hdfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;

/**
 * 大文件随机访问，可以实现断点续传功能等
 * 20200612 by hecj
 */
public class RandomAccessFileTest {
    public static void main(String[] args) throws Exception {
//        read1();
        read2();
    }

    /**
     * 先读取一段内容
     * @throws Exception
     */
    public static void read1() throws Exception {
        RandomAccessFile raf = new RandomAccessFile("/Users/hecj/Desktop/file3/1.docx","r");
        FileOutputStream os = new FileOutputStream("/Users/hecj/Desktop/file3/13.docx");
        byte[] bs = new byte[11100];
        raf.read(bs,0,bs.length-1);
        os.write(bs);
        os.close();
        raf.close();
    }

    /**
     * 再读取另外一段，追加写入
     * @throws Exception
     */
    public static void read2() throws Exception {
        RandomAccessFile raf = new RandomAccessFile("/Users/hecj/Desktop/file3/1.docx","r");
        // 第二个参数是true，表示追加写入
        FileOutputStream os = new FileOutputStream("/Users/hecj/Desktop/file3/13.docx",true);
        raf.seek(11100);
        byte[] bs = new byte[39];
        raf.read(bs,0,bs.length);
        os.write(bs);
        os.close();
        raf.close();
    }

}
