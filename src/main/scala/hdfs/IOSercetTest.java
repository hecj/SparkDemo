package hdfs;

import java.io.*;

/**
 * 文件加密与解密
 * 20200610 by hecj
 */
public class IOSercetTest {
	static String fileName ="/Users/hecj/Desktop/file2/1.txt";
	static String secretFileName ="/Users/hecj/Desktop/file2/1.txt.sec";
	static String decryptFileName ="/Users/hecj/Desktop/file2/1.decrypt.txt";
	public static void main(String[] args) throws IOException {
		secret(fileName,secretFileName);
		decrypt(secretFileName,decryptFileName);
	}

    /**
	 * 加密
	 */
	public static void secret(String fileName,String decryptFileName) throws IOException{
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(fileName));
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(decryptFileName));
		int len;
		long a=System.currentTimeMillis();
		byte[] bs = new byte[1024*1024*64];
		while((len=bis.read(bs))!=-1){
			// 一次读取一个字节 0-255之间 , 1个字节是8位
			for(int i=0;i<len&&i<20;i++){
				bs[i] = (byte)(bs[i]+1);
			}
			bos.write(bs,0,len);
		}
		long b=System.currentTimeMillis();
		bis.close();
		bos.close();
		System.out.println("加密拷贝成功！");
		System.out.println("加密用时："+(b-a)+"ms");
	}

    /**
	 * 解密
	 */
	public static void decrypt(String secretFileName,String decryptFileName) throws IOException{
		BufferedInputStream bis = new BufferedInputStream(new FileInputStream(secretFileName));
		BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(decryptFileName));
		int len;
		long a=System.currentTimeMillis();
		byte[] bs = new byte[1024*1024*64];
		while((len=bis.read(bs))!=-1){
			// 一次读取一个字节 0-255之间 , 1个字节是8位
			for(int i=0;i<len&&i<20;i++){
				bs[i] = (byte)(bs[i]-1);
			}
			bos.write(bs,0,len);
		}
		long b=System.currentTimeMillis();
		bis.close();
		bos.close();
		System.out.println("解密拷贝成功！");
		System.out.println("解密用时："+(b-a)+"ms");
	}
}