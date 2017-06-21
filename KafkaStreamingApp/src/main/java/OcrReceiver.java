import java.util.*;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;


public class OcrReceiver {

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

        JavaDStream<String> ocrOutput = messages.map(
                new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) throws Exception {
                String imgFilename=tuple2._1();
                if(imgFilename==null || imgFilename.equals("")) return "Invalid image path";
                String imgName=OcrReceiverUtils.getImgName(imgFilename);
                String imgPath=OcrReceiverUtils.TMP_IMG_PATH+imgName;
                String imgStr=tuple2._2();
                OcrReceiverUtils.generateImg(imgStr,imgPath);
                return OcrReceiverUtils.ocrProcessAndWriteLocal(imgPath);
            }
        });

        ocrOutput.print();

        jssc.start();
        jssc.awaitTermination();
    }

}
