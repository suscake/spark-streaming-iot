import java.io.*;

public class MoveFiles {
    public static void main(String[] args)throws IOException{
        String fromPathPrefix="/home/jiacheng/testdata/type2_test1/";
        String IMG_PATH_PHY_PREFIX="/mnt/DP_disk";
        String IMG_PATH_PHY_SUFFIX="/jiacheng/testdata/";
        InputStream in =null;
        byte[] data=null;
        for(int i=1;i<=10000;i++){
            String imgName="type2_test1_"+i+".jpg";
            in=new FileInputStream(fromPathPrefix+imgName);
            data=new byte[in.available()];
            in.read(data);
            in.close();
            int diskId=i%7+1;
            String imgPath=IMG_PATH_PHY_PREFIX+diskId+IMG_PATH_PHY_SUFFIX+imgName;
            OutputStream out =new FileOutputStream(imgPath);
            out.write(data);
            out.flush();
            out.close();
            if(i%100==0) System.out.println("finished 100 files until "+i);
        }
    }
}
