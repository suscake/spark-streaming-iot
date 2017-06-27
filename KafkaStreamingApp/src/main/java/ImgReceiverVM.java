
public class ImgReceiverVM extends ImgReceiver {

    public static void main(String[] args) throws Exception{
        if (args.length != 4) {
            System.err.println("args: <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }
        ImgReceiverVM receiverVM=new ImgReceiverVM(args[0],args[1],args[2],args[3]);
        receiverVM.startReceiver();
    }

    public ImgReceiverVM(String zkQuorum,String group,String topics,String numThreads){
        super(zkQuorum, group, topics, numThreads);
    }

    @Override
    public String getTmpImgPath(String imgFilename){
        return "/home/centos/tmp-ocr/img/"+imgFilename;
    }

    @Override
    public String getOutputDir(String imgFilename){
        return "/home/centos/tmp-ocr/output/"+imgFilename.substring(0,imgFilename.length()-4)+".txt";
    }

    @Override
    public String getLogPath(){
        return "/home/centos/tmp-ocr/log"+logTimestamp+".txt";
    }

}
