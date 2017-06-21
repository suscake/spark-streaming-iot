import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.producer.*;
import java.util.*;
import java.io.*;
import java.util.concurrent.*;

public class KafkaProducers implements Runnable {

    public static void main(String[] args) throws Exception{
        int numThreads=2;
        int num=3;
        ExecutorService executor=Executors.newCachedThreadPool();
        for(int i=0;i<numThreads;i++){
            executor.execute(new KafkaProducers("cjc2:9092,cjc3:9092,cjc4:9092",
                    "testzk1",i*num+1,num));
        }

    }

    private String brokers;
    private String topic;
    private int offset;
    private int num;
    private final String IMG_PATH= "/home/centos/ocr-sample/testdata/type2_test1/";

    public KafkaProducers(String brokers,String topic,int offset,int num){
        this.brokers=brokers;
        this.topic=topic;
        this.offset=offset;
        this.num=num;
    }


    public void run()  {
        Properties props = new Properties();
        props.put("bootstrap.servers",brokers);
        props.put("acks","all");
        props.put("retries",0);
        props.put("batch.size",1638400);
        props.put("linger.ms",1);
        props.put("buffer.memory",335544320);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);

        try{
            for(int i = offset; i < offset+num; i++){
                String imgFilename=getImgFilename(i);
                String keyStr="NAME: " + imgFilename+" TIMESTAMP: "+System.currentTimeMillis();
                System.out.println(keyStr);
                String imgStr=getImgStr(imgFilename);
                producer.send(new ProducerRecord<>(topic, keyStr, imgStr));
                Thread.sleep(1000);
            }
        }catch (Exception e){
            e.printStackTrace();
        }


        producer.close();
    }

    private String getImgFilename(int id){
        return "type2_test1_"+id+".jpg";
    }

    private String getImgStr(String fileName){
        InputStream in =null;
        byte[] data=null;
        String imgPath=IMG_PATH+fileName;
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
