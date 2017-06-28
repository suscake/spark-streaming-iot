import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.producer.*;

import java.util.*;
import java.io.*;

public abstract class ImgProducer implements Runnable {

    private String brokers;
    private String topic;
    private int offset;
    private int num;
    private int producerId;
/*
    public ImgProducer(String brokers,String topic,int offset,int num){
        this.brokers=brokers;
        this.topic=topic;
        this.offset=offset;
        this.num=num;
    }
   */

    public ImgProducer(String brokers, String topic, int producerId, int num) {
        this.brokers = brokers;
        this.topic = topic;
        this.producerId = producerId;
        this.num = num;
    }

/*
    //send base64-encoded images
    public void run() {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            long t0 = System.currentTimeMillis();
            int producerId = (offset - 1) / num;
            for (int i = offset; i < offset + num; i++) {
                long t1 = System.currentTimeMillis();
                String imgFilename = getImgFilename(i);
                String keyStr = imgFilename + " " + t1;
                String imgStr = getImgStr(i);
                producer.send(new ProducerRecord<>(topic, keyStr, imgStr));
                long t2 = System.currentTimeMillis();
                long delta = t2 - t1;
                System.out.println("PRODUCER " + producerId + " SENT " + imgFilename + " IN " + delta + "ms");
                Thread.sleep(1000);
            }
            long t4 = System.currentTimeMillis();
            long deltaTotal = t4 - t0 - 1000 * num;
            System.out.println("PRODUCER " + producerId + " SENT " + num + " IMGS IN " + deltaTotal + "ms");
        } catch (Exception e) {
            e.printStackTrace();
        }

        producer.close();
    }
*/

/*
    public String getImgFilename(int id) {
        return "type2_test1_" + id + ".jpg";
    }
*/
    public String getImgFilename(int id) {
        return id +".jpg";
    }

    public abstract String getImgPath(int id);

/*
    private String getImgStr(int id) {
        InputStream in = null;
        byte[] data = null;
        String imgPath = getImgPath(id);
        try {
            in = new FileInputStream(imgPath);
            data = new byte[in.available()];
            in.read(data);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Base64.encodeBase64String(data);
    }
*/

    private byte[] getImgBytes(int id) {
        InputStream in = null;
        byte[] data = null;
        String imgPath = getImgPath(id);
        try {
            in = new FileInputStream(imgPath);
            data = new byte[in.available()];
            in.read(data);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

    /*
    //send images without base64 encoding, i.e., send byte array directly
    public void run()  {
        Properties props = new Properties();
        props.put("bootstrap.servers",brokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        Producer<String, byte[]> producer = new KafkaProducer<>(props);

        try{
            long t0=System.currentTimeMillis();
            int producerId=(offset-1)/num;
            for(int i = offset; i < offset+num; i++){
                long t1=System.currentTimeMillis();
                String imgFilename=getImgFilename(i);
                String keyStr=imgFilename+" "+t1;
                byte[] imgBytes=getImgBytes(i);
                producer.send(new ProducerRecord<>(topic, keyStr, imgBytes));
                long t2=System.currentTimeMillis();
                long delta=t2-t1;
                System.out.println("PRODUCER "+producerId+" SENT "+imgFilename+" IN "+delta+"ms");
                Thread.sleep(1000);
            }
            long t4=System.currentTimeMillis();
            long deltaTotal=t4-t0-1000*num;
            System.out.println("PRODUCER "+producerId+" SENT "+num+" IMGS IN "+deltaTotal+"ms");
        }catch (Exception e){
            e.printStackTrace();
        }

        producer.close();
    }
*/

    //send 1024*768 images in bytes
    public void run() {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        Producer<String, byte[]> producer = new KafkaProducer<>(props);

        try {
            for (int i = 0; i < num; i++) {
                long t1 = System.currentTimeMillis();
                int imgId=producerId % 10 + 1;
                String renamedImgName=imgId+"_prod"+producerId+"_no"+i+".jpg";
                String keyStr = renamedImgName + " " + t1;
                byte[] imgBytes = getImgBytes(imgId);
                producer.send(new ProducerRecord<>(topic, keyStr, imgBytes));
                long t2 = System.currentTimeMillis();
                System.out.println("PRODUCER " + producerId + " SENT " + renamedImgName + " IN " + (t2 - t1) + "ms");
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        producer.close();
    }


}
