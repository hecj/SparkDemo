package hdfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * 文件加密与解密
 * 20200610 by hecj
 */
public class IOSercet {
	static String fileName ="/Users/hecj/Desktop/file2/1.mp4";
	static String secretFileName ="/Users/hecj/Desktop/file2/1.mp4.sec";
	static String decryptFileName ="/Users/hecj/Desktop/file2/1.decrypt.mp4";
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
		int n;
		long a=System.currentTimeMillis();
		while((n=bis.read())!=-1){
//			System.out.println(n);
			// 一次读取一个字节 0-255之间 , 1个字节是8位
			// byte转二进制字符串
//			String tString = Integer.toBinaryString((n & 0xFF) + 0x100).substring(1);
//			System.out.println(n+"_"+tString);
			bos.write(n+1);
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
		int n;
		long a=System.currentTimeMillis();
		while((n=bis.read())!=-1){
			bos.write(n-1);
		}
		long b=System.currentTimeMillis();
		bis.close();
		bos.close();
		System.out.println("解密拷贝成功！");
		System.out.println("解密用时："+(b-a)+"ms");
	}
}