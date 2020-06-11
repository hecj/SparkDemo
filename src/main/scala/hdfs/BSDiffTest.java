package hdfs;

import com.tencent.tinker.bsdiff.BSDiff;
import com.tencent.tinker.bsdiff.BSPatch;

import java.io.File;

public class BSDiffTest {

    public static void main(String[] args) throws Exception {
        File oldFile = new File("/Users/hecj/Desktop/file3/1.docx");
        File newFile = new File("/Users/hecj/Desktop/file3/2.docx");
        File diffFile = new File("/Users/hecj/Desktop/file3/1.patch");
//        BSDiff.bsdiff(oldFile,newFile,diffFile);

        BSPatch.patchFast(oldFile,new File("/Users/hecj/Desktop/file3/1_new.docx"),diffFile,0);
    }
}
