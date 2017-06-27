import java.io.*;
import org.apache.commons.codec.binary.Base64;

import java.util.*;

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

    public ImgReceiver(String zkQuorum,String group,String topics,String numThreads){
        this.zkQuorum=zkQuorum;
        this.group=group;
        this.topics=topics;
        this.numThreads=numThreads;
    }

    public abstract String getTmpImgPath(String imgFilename);



    public abstract String getOutputDir(String imgFilename);

    public boolean generateImg(String imgStr,String path){
        if(imgStr==null) return false;
        try{
            byte[] b=Base64.decodeBase64(imgStr);
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

    public String ocrProcess(String imgPath){
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

    public String ocrProcessAndWriteLocal(String imgFilename, String imgPath,String timestamp){
        try{
            String output=ocrProcess(imgPath);
            long t=System.currentTimeMillis();
            long t0=Long.parseLong(timestamp);
            long delta=t-t0;
            output+=imgPath+": finish in "+delta+"ms\n";
            String outputPath=getOutputDir(imgFilename);
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
                        String timestamp=keyElem[1];
                        String imgPath=getTmpImgPath(imgFilename);
                        String imgStr=tuple2._2();
                        generateImg(imgStr,imgPath);
                        return ocrProcessAndWriteLocal(imgFilename,imgPath,timestamp);
                    }
                });

        ocrOutput.print();

        jssc.start();
        jssc.awaitTermination();

    }



}






















