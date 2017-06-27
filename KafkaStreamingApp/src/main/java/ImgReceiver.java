import java.io.*;
import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.apache.commons.codec.binary.Base64;

import java.util.*;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

public abstract class ImgReceiver implements Serializable{

    private String zkQuorum;
    private String group;
    private String topics;
    private String numThreads;
    public long logTimestamp;
    private String brokers;

    public ImgReceiver(String zkQuorum,String group,String topics,String numThreads){
        this.zkQuorum=zkQuorum;
        this.group=group;
        this.topics=topics;
        this.numThreads=numThreads;
        this.logTimestamp=System.currentTimeMillis();
    }

    public ImgReceiver(String brokers,String topics){
        this.brokers=brokers;
        this.topics=topics;
        this.logTimestamp=System.currentTimeMillis();
    }

    public abstract String getTmpImgPath(String imgFilename);

    public abstract String getOutputDir(String imgFilename);

    public abstract String getLogPath();

    public boolean generateImg(String imgStr,String imgFilename){
        if(imgStr==null) return false;
        try{
            byte[] b=Base64.decodeBase64(imgStr);
            OutputStream out =new FileOutputStream(getTmpImgPath(imgFilename));
            out.write(b);
            out.flush();
            out.close();
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    public boolean generateImgFromBytes(byte[] imgBytes,String imgFilename){
        try{
            OutputStream out =new FileOutputStream(getTmpImgPath(imgFilename));
            out.write(imgBytes);
            out.flush();
            out.close();
            return true;
        }catch (Exception e){
            e.printStackTrace();
            return false;
        }
    }

    public String ocrProcess(String imgFilename){
        try {
            String imgPath=getTmpImgPath(imgFilename);
            String ocrOutputPath=getOutputDir(imgFilename);
            String cmd = "tesseract " + imgPath + " "+ocrOutputPath.substring(0,ocrOutputPath.length()-4);
            Process process = Runtime.getRuntime().exec(cmd);
            return imgFilename+" ocr result saved in "+ocrOutputPath+"!\n";
        }catch (Exception e){
            e.printStackTrace();
            return imgFilename+" OCR ERROR!\n";
        }
    }

    public String ocrProcessAndWriteLocal(String imgFilename,List<Long> tList){
        try{
            String output=ocrProcess(imgFilename);
            long tOcr=System.currentTimeMillis();
            tList.add(tOcr);
            output+="receive in "+(tList.get(1)-tList.get(0))+"ms\n";
            output+="save img in "+(tList.get(2)-tList.get(1))+"ms\n";
            output+="get ocr result in "+(tList.get(3)-tList.get(2))+"ms\n";
            String logPath=getLogPath();
            File f = new File(logPath);
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


    public void startReceiver() throws Exception{
        SparkConf sparkConf = new SparkConf().setAppName("OcrStreamingApp")
                .set("spark.streaming.blockInterval","50");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(1000));

        Map<String, Integer> topicMap = new HashMap<>();
        String[] topicList = topics.split(",");
        for (String topic: topicList) {
            topicMap.put(topic, Integer.parseInt(numThreads));
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, zkQuorum, group, topicMap);

        JavaDStream<String> ocrOutput = messages.map(
                new Function<Tuple2<String, String>, String>() {
                    @Override
                    public String call(Tuple2<String, String> tuple2) throws Exception {
                        String keyStr=tuple2._1();
                        String[] keyElem=keyStr.split(" ");
                        String imgFilename = keyElem[0];
                        String tSend=keyElem[1];
                        String imgStr=tuple2._2();
                        List<Long> timestampList=new ArrayList<>();
                        timestampList.add(Long.parseLong(tSend));
                        long tReceive=System.currentTimeMillis();
                        timestampList.add(tReceive);
                        generateImg(imgStr,imgFilename);
                        long tSaveImg=System.currentTimeMillis();
                        timestampList.add(tSaveImg);
                        return ocrProcessAndWriteLocal(imgFilename,timestampList);
                    }
                });

        ocrOutput.print();

        jssc.start();
        jssc.awaitTermination();
    }

    public void startDirectReceiver() throws Exception{
        SparkConf sparkConf = new SparkConf().setAppName("OcrStreamingApp")
                .set("spark.streaming.blockInterval","50");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        JavaDStream<String> ocrOutput = messages.map(
                new Function<Tuple2<String, String>, String>() {
                    @Override
                    public String call(Tuple2<String, String> tuple2) throws Exception {
                        String keyStr=tuple2._1();
                        String[] keyElem=keyStr.split(" ");
                        String imgFilename = keyElem[0];
                        String tSend=keyElem[1];
                        String imgStr=tuple2._2();
                        List<Long> timestampList=new ArrayList<>();
                        timestampList.add(Long.parseLong(tSend));
                        long tReceive=System.currentTimeMillis();
                        timestampList.add(tReceive);
                        generateImg(imgStr,imgFilename);
                        long tSaveImg=System.currentTimeMillis();
                        timestampList.add(tSaveImg);
                        return ocrProcessAndWriteLocal(imgFilename,timestampList);
                    }
                });

        ocrOutput.print();

        jssc.start();
        jssc.awaitTermination();
    }

    public void startDirectReceiverInBytes() throws Exception{
        SparkConf sparkConf = new SparkConf().setAppName("OcrStreamingApp")
                .set("spark.streaming.blockInterval","50");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, byte[]> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                byte[].class,
                StringDecoder.class,
                DefaultDecoder.class,
                kafkaParams,
                topicsSet
        );

        JavaDStream<String> ocrOutput = messages.map(
                new Function<Tuple2<String, byte[]>, String>() {
                    @Override
                    public String call(Tuple2<String, byte[]> tuple2) throws Exception {
                        String keyStr=tuple2._1();
                        String[] keyElem=keyStr.split(" ");
                        String imgFilename = keyElem[0];
                        String tSend=keyElem[1];
                        byte[] imgBytes=tuple2._2();
                        List<Long> timestampList=new ArrayList<>();
                        timestampList.add(Long.parseLong(tSend));
                        long tReceive=System.currentTimeMillis();
                        timestampList.add(tReceive);
                        generateImgFromBytes(imgBytes,imgFilename);
                        long tSaveImg=System.currentTimeMillis();
                        timestampList.add(tSaveImg);
                        return ocrProcessAndWriteLocal(imgFilename,timestampList);
                    }
                });

        ocrOutput.print();

        jssc.start();
        jssc.awaitTermination();
    }


}






















