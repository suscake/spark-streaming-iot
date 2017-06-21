import java.util.*;
import java.text.SimpleDateFormat;
import java.io.*;

import com.datastax.spark.connector.japi.CassandraStreamingJavaUtil;
import com.datastax.spark.connector.japi.CassandraJavaUtil;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;




public class OcrReceiverCassandra {

    public static void main(String[] args) throws Exception {
        int batchInterval=5000;

        if (args.length < 4) {
            System.err.println("args: <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setAppName("OcrStreamingApp");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf,
                new Duration(batchInterval));

        int numThreads = Integer.parseInt(args[3]);
        Map<String, Integer> topicMap = new HashMap<>();
        String[] topics = args[2].split(",");
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }

        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, args[0], args[1], topicMap);

        JavaDStream<OcrRow> ocrOutput=messages.map(
                new Function<Tuple2<String, String>, OcrRow>() {
            @Override
            public OcrRow call(Tuple2<String, String> tuple2) throws Exception {
                String imgFilename=tuple2._1();
                if(imgFilename==null || imgFilename.equals("")) return null;
                String imgName=OcrReceiverUtils.getImgName(imgFilename);
                String imgPath=OcrReceiverUtils.TMP_IMG_PATH+imgName;
                String imgStr=tuple2._2();
                OcrReceiverUtils.generateImg(imgStr,imgPath);
                String output=OcrReceiverUtils.ocrProcess(imgPath);
                SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss aa");
                String timeStamp=sdf.format(new Date());
                return new OcrRow(imgName,output,timeStamp);
            }
        });

        CassandraStreamingJavaUtil.javaFunctions(ocrOutput).writerBuilder("ocrdata",
                "ocr",CassandraJavaUtil.mapToRow(OcrRow.class)).saveToCassandra();


        ocrOutput.print();

        jssc.start();
        jssc.awaitTermination();
    }



    public static class OcrRow implements Serializable{
        private String imgId;
        private String output;
        private String timeStamp;

        public OcrRow(){}

        public OcrRow(String imgId,String output,String timeStamp){
            this.imgId=imgId;
            this.output=output;
            this.timeStamp=timeStamp;
        }

        public String getImgId(){return imgId;}
        public void setImgId(String imgId){this.imgId=imgId;}

        public String getOutput(){return output;}
        public void setOutput(String output){this.output=output;}

        public String getTimeStamp(){return timeStamp;}
        public void setTimeStamp(String timeStamp){this.timeStamp=timeStamp;}

        public String toString(){
            return output;
        }
    }
}
