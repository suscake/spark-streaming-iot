public class ImgReceiverPHY extends ImgReceiver {

    public static void main(String[] args) throws Exception{
        if (args.length != 4) {
            System.err.println("args: <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }
        ImgReceiverPHY receiverPHY=new ImgReceiverPHY(args[0],args[1],args[2],args[3]);
        receiverPHY.startReceiver();
    }

    public ImgReceiverPHY(String zkQuorum,String group,String topics,String numThreads){
        super(zkQuorum, group, topics, numThreads);
    }

    @Override
    public String getTmpImgPath(String imgFilename){
        int diskId=getDiskId(imgFilename);
        return "/mnt/DP_disk"+diskId+"/jiacheng/ocr-tmp/img/"+imgFilename;
    }

    @Override
    public String getOutputDir(String imgFilename){
        int diskId=getDiskId(imgFilename);
        return "/mnt/DP_disk"+diskId+"/jiacheng/ocr-tmp/output.txt";
    }

    private int getDiskId(String imgFilename){
        String imgIdStr=imgFilename.substring(12,imgFilename.length()-4);
        int imgId=Integer.parseInt(imgIdStr);
        return imgId%7+1;
    }
}
