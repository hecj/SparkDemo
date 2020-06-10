package hdfs;
 
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
/***
 *  文件的切割和合并 
 * 1.要切割和合并文件：主要考虑的就是文件的源地址，目标地址，暂存文件地址和文件名称 
 * 2.切割文件：判断给的暂存地址是否存在，不存在，则创建；从源地址中读出文件，按照给定的大小进行文件的切割操作放入暂存地址中 
 * 3.合并文件：判断给定的目标地址是否存在，不存在，则创建；定义List集合将暂存地址中的文件全部读取出来，放到list集合中 
 *          然后使用Enumeration列举出所有文件，合并流合并文件 
 *          最后写入到目标的地址中 
 * 
 * @author zw
 *
 */
public class SplitFile {
	public static void main(String[] args) throws IOException {
		File srcFile = new File("/Users/hecj/Desktop/file2/1.mp4");
		File tempFile = new File("/Users/hecj/Desktop/file2/分割临时文件");
		File dirFile = new File("/Users/hecj/Desktop/file2/合并文件");
		String fileName = srcFile.getName();
		cut(srcFile,tempFile,1024*1024);//按照30字节切割
		unionFile(tempFile, dirFile, fileName);
		
	}
	
	/***
	 * 切割文件
	 * 
	 * @param srcFile
	 * @param tempFile
	 * @throws IOException 
	 */
	public static void cut(File srcFile, File tempFile,int cutByte/*按照cutByte字节切割*/) throws IOException {
		
		 //读取源地址文件  
        FileInputStream fis = new FileInputStream(srcFile);  
        FileOutputStream fos = null;  
        //是否为文件，不是就创建  
        if(!tempFile.isFile()){  
            tempFile.mkdirs();  
        }  
      
        byte[] bs = new byte[cutByte];
        int len =0;  
        int count = 0;  
        while((len=fis.read(bs)) != -1){  
            int num = count++;  
            //写入暂存地址目录中  
            fos = new FileOutputStream(new File(tempFile, num+".part"));  
            fos.write(bs, 0, len);  
              
        }  
        fos.flush();  
        fos.close();  
        fis.close();  
          
        System.out.println("分割完成！");  
	}
	/***
	 * 合并文件
	 * @throws IOException 
	 */
	public static void unionFile(File tempFile,File dirFile,String fileName) throws IOException {
		if(!dirFile.isFile()) {
			dirFile.mkdirs();
		}
		List<FileInputStream> list = new ArrayList<FileInputStream>();
		File[] f =tempFile.listFiles();
		
		
		for(int i =0;i<f.length;i++) {
			list.add(new FileInputStream(new File(tempFile, i+".part")));
		}
		//将文件全部列取出来
		Enumeration<FileInputStream> eum = Collections.enumeration(list);
		
		 SequenceInputStream sis = new SequenceInputStream(eum); 
		 FileOutputStream fos= new FileOutputStream(new File(dirFile,fileName));
		 
		 byte[] bte = new byte[1024];
		 int len;
		 while(-1!=(len= sis.read(bte))) {
			 fos.write(bte, 0, len);
		 }
		 fos.flush();
		 fos.close();
		 sis.close();
		 System.out.println("提示信息：文件合并完毕！");
	}
	
}