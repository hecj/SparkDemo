package hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * HDFS测试
 * 
 * @author zhi
 * @since 2019年9月10日18:28:24
 *
 */
public class HadoopTest {
    private FileSystem fileSystem = null;

    @Before
    public void before() throws Exception {
//        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.8.3");
        Configuration configuration = new Configuration();
        Configuration conf = new Configuration();
        conf.set("dfs.blocksize", "30k");
        fileSystem = FileSystem.get(new URI("hdfs://localhost:9000"), configuration, "root");
    }

    @After
    public void after() throws Exception {
        if (fileSystem != null) {
            fileSystem.close();
        }
    }

    /**
     * 创建文件夹
     */
    @Test
    public void mkdir() {
        try {
            boolean result = fileSystem.mkdirs(new Path("/test"));
            System.out.println("创建文件夹结果："+ result);
        } catch (IllegalArgumentException | IOException e) {
            System.out.println("创建文件夹出错"+ e);
        }
    }

    /**
     * 上传文件
     */
    @Test
    public void uploadFile() {
        String fileName = "3.pdf";
        InputStream input = null;
        OutputStream output = null;
        try {
            input = new FileInputStream("/Users/hecj/Desktop/" + fileName);
            output = fileSystem.create(new Path("/test/3.2.pdf"));
            IOUtils.copyBytes(input, output, 100, true);
            System.out.println("上传文件成功");
        } catch (IllegalArgumentException | IOException e) {
            System.out.println("上传文件出错"+ e);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                }
            }
            if (output != null) {
                try {
                    output.close();
                } catch (IOException e) {
                }
            }
        }
    }

    /**
     * 下载文件
     */
    @Test
    public void downFile() {
        String fileName = "hadoop.txt";
        InputStream input = null;
        OutputStream output = null;
        try {
            input = fileSystem.open(new Path("/test/" + fileName));
            output = new FileOutputStream("F:\\down\\" + fileName);
            IOUtils.copyBytes(input, output, 4096, true);
            System.out.println("下载文件成功");
        } catch (IllegalArgumentException | IOException e) {
            System.out.println("下载文件出错"+e);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                }
            }
            if (output != null) {
                try {
                    output.close();
                } catch (IOException e) {
                }
            }
        }
    }

    /**
     * 删除文件
     */
    @Test
    public void deleteFile() {
        String fileName = "hadoop.txt";
        try {
            boolean result = fileSystem.delete(new Path("/test/" + fileName), true);
            System.out.println("删除文件结果：{}"+result);
        } catch (IllegalArgumentException | IOException e) {
            System.out.println("删除文件出错"+ e);
        }
    }

    /**
     * 遍历文件
     */
    @Test
    public void listFiles() {
        try {
            FileStatus[] statuses = fileSystem.listStatus(new Path("/"));
            for (FileStatus file : statuses) {
                System.out.println("扫描到文件或目录，名称：{}，是否为文件：{}"+file.getPath().getName()+file.isFile());
            }
        } catch (IllegalArgumentException | IOException e) {
            System.out.println("遍历文件出错"+e);
        }
    }
}