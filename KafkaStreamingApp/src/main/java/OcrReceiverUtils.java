import java.io.*;
import org.apache.commons.codec.binary.Base64;

public class OcrReceiverUtils {

    public static String getImgName(String path){
        String[] dirs=path.split("/");
        return dirs[dirs.length-1];
    }

    public static String getTmpImgPath(String imgFilename,String mode){
        if(mode.equals("VM"))
            return "/home/centos/tmp-ocr/img/"+imgFilename;
        else if(mode.equals("PHY")){
            int diskId=getDiskId(imgFilename);
            return "/mnt/DP_disk"+diskId+"/jiacheng/ocr-tmp/img/"+imgFilename;
        }else{
            System.err.println("mode must be VM or PHY");
            System.exit(1);
            return null;
        }
    }

    private static int getDiskId(String imgFilename){
        String imgIdStr=imgFilename.substring(12,imgFilename.length()-4);
        int imgId=Integer.parseInt(imgIdStr);
        return imgId%7+1;
    }

    public static String getOutputDir(String imgFilename,String mode){
        if(mode.equals("VM"))
            return "/home/centos/tmp-ocr/output/output.txt";
        else if(mode.equals("PHY")){
            int diskId=getDiskId(imgFilename);
            return "/mnt/DP_disk"+diskId+"/jiacheng/ocr-tmp/output.txt";
        }else{
            System.err.println("mode must be VM or PHY");
            System.exit(1);
            return null;
        }
    }

    public static boolean generateImg(String imgStr,String path){
        if(imgStr==null) return false;
        try{
            byte[] b=Base64.decodeBase64(imgStr);
            /*
            for(int i=0;i<b.length;i++){
                if (b[i]<0) b[i]+=256;
            }
            */
            OutputStream out =new FileOutputStream(path);
            out.write(b);
            out.flush();
            out.close();
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    public static String ocrProcess(String imgPath){
        try {
            String cmd = "tesseract " + imgPath + " stdout";
            Process process = Runtime.getRuntime().exec(cmd);
            InputStreamReader ir = new InputStreamReader(process.getInputStream());
            LineNumberReader input = new LineNumberReader(ir);

            String line;
            StringBuilder outputStr = new StringBuilder();
            if ((line = input.readLine()) == null) {
                outputStr.append(imgPath + ": OCR failed!\n");
            } else {
                int lineNum = 0;
                do {
                    if (line.equals("")) continue;
                    lineNum += 1;
                    outputStr.append(imgPath + ": line-" + lineNum + ": " + line + "\n");
                } while ((line = input.readLine()) != null);
            }
            String output = outputStr.toString();
            return output;
        }catch (Exception e){
            e.printStackTrace();
            return "OCR ERROR!"+imgPath;
        }
    }

    public static String ocrProcessAndWriteLocal(String imgFilename,
                                                 String imgPath,String mode,String timestamp){
        try{
            String output=ocrProcess(imgPath);
            long t=System.currentTimeMillis();
            long t0=Long.parseLong(timestamp);
            long delta=t-t0;
            output+=imgPath+": finish in "+delta+"ms\n";
            String outputPath=getOutputDir(imgFilename,mode);
            File f = new File(outputPath);
            if (!f.exists()) f.createNewFile();
            FileWriter fw = new FileWriter(f, true);
            fw.write(output);
            fw.flush();
            fw.close();
            return output;
        }catch (Exception e){
            e.printStackTrace();
            return "OCR ERROR!";
        }
    }



    public static final String TMP_IMG_PATH="/home/centos/tmp-ocr/img/";
}
