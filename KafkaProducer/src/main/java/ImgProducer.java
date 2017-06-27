import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.producer.*;
import java.util.*;
import java.io.*;

public abstract class ImgProducer implements Runnable {

    private String brokers;
    private String topic;
    private int offset;
    private int num;
    private final String IMG_PATH_VM= "/home/centos/ocr-sample/testdata/type2_test1/";
    private final String IMG_PATH_PHY_PREFIX="/mnt/DP_disk";
    private final String IMG_PATH_PHY_SUFFIX="/jiacheng/testdata/";
    private final int DISK_NUM=7;
    private final int SLEEP_TIME_MS=1000;

    public ImgProducer(String brokers,String topic,int offset,int num){
        this.brokers=brokers;
        this.topic=topic;
        this.offset=offset;
        this.num=num;
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
                long t1=System.currentTimeMillis();
                String imgFilename=getImgFilename(i);
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

    public String getImgFilename(int id){
        return "type2_test1_"+id+".jpg";
    }
/*
    private String getImgPath(int id){
        String imgFilename=getImgFilename(id);
        if(mode.equals("VM")) {
            return IMG_PATH_VM+imgFilename;
        }
        else if(mode.equals("PHY")){
            int diskId=id%DISK_NUM+1;
            return IMG_PATH_PHY_PREFIX+diskId+IMG_PATH_PHY_SUFFIX+imgFilename;
        }else{
            System.err.println("mode must be VM or PHY");
            System.exit(1);
            return null;
        }
    }
*/
    public abstract String getImgPath(int id);



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
