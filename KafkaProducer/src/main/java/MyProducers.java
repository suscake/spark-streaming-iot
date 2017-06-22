import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.producer.*;
import java.util.*;
import java.io.*;
import java.util.concurrent.*;

public class MyProducers implements Runnable {

    public static void main(String[] args) throws Exception{

        //args description:
        //brokers: VM:cjc2:9092, PHY:localhost:9092
        //topic: VM: testzk1, PHY: test-7-pars
        //num: the number of messages each producer sends
        //numThreads: equals number of producers launched
        //mode: VM or PHY
        if (args.length != 5) {
            System.err.println("args: <brokers> <topic> <num> <numThreads> <mode>");
            System.exit(1);
        }

        String brokers=args[0];
        String topic=args[1];
        int num=Integer.parseInt(args[2]);
        int numThreads=Integer.parseInt(args[3]);
        String mode=args[4];

        ExecutorService executor=Executors.newCachedThreadPool();
        for(int i=0;i<numThreads;i++){
            executor.execute(new MyProducers(brokers, topic,i*num+1,num,mode));
        }

    }

    private String brokers;
    private String topic;
    private int offset;
    private int num;
    private String mode;
    private final String IMG_PATH_VM= "/home/centos/ocr-sample/testdata/type2_test1/";
    private final String IMG_PATH_PHY_PREFIX="/mnt/DP_disk";
    private final String IMG_PATH_PHY_SUFFIX="/jiacheng/testdata/";
    private final int DISK_NUM=7;
    private final int SLEEP_TIME_MS=1000;

    public MyProducers(String brokers,String topic,int offset,int num,String mode){
        this.brokers=brokers;
        this.topic=topic;
        this.offset=offset;
        this.num=num;
        this.mode=mode;
    }


    public void run()  {
        Properties props = new Properties();
        props.put("bootstrap.servers",brokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);

        try{
            long t0=System.currentTimeMillis();
            int producerId=(offset-1)/num;
            for(int i = offset; i < offset+num; i++){
                String imgFilename=getImgFilename(i);
                long t1=System.currentTimeMillis();
                String keyStr=imgFilename+" "+t1;
                String imgStr=getImgStr(i);
                producer.send(new ProducerRecord<>(topic, keyStr, imgStr));
                long t2=System.currentTimeMillis();
                long delta=t2-t1;
                System.out.println("PRODUCER "+producerId+" SENT "+imgFilename+" IN "+delta+"ms");
                Thread.sleep(SLEEP_TIME_MS);
            }
            long t4=System.currentTimeMillis();
            long deltaTotal=t4-t0-SLEEP_TIME_MS*num;
            System.out.println("PRODUCER "+producerId+" SENT "+num+" IMGS IN "+deltaTotal+"ms");
        }catch (Exception e){
            e.printStackTrace();
        }

        producer.close();
    }

    private String getImgFilename(int id){
        return "type2_test1_"+id+".jpg";
    }

    private String getImgPath(int id){
        String imgFilename=getImgFilename(id);
        if(mode.equalsIgnoreCase("VM")) {
            return IMG_PATH_VM+imgFilename;
        }
        else if(mode.equalsIgnoreCase("PHY")){
            int diskId=id%DISK_NUM+1;
            return IMG_PATH_PHY_PREFIX+diskId+IMG_PATH_PHY_SUFFIX+imgFilename;
        }else{
            System.err.println("mode must be VM or PHY");
            System.exit(1);
            return null;
        }
    }

    private String getImgStr(int id){
        InputStream in =null;
        byte[] data=null;
        String imgPath=getImgPath(id);
        try{
            in=new FileInputStream(imgPath);
            data=new byte[in.available()];
            in.read(data);
            in.close();
        }catch (IOException e){
            e.printStackTrace();
        }
        return Base64.encodeBase64String(data);
    }

}
