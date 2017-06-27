import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ImgProducerPHY extends ImgProducer {

    public static void main(String[] args) {
        //args description:
        //brokers: VM:cjc2:9092, PHY:localhost:9092
        //topic: VM: testzk1, PHY: test-7-pars
        //num: the number of messages each producer sends
        //numThreads: equals number of producers launched
        if (args.length != 4) {
            System.err.println("args: <brokers> <topic> <num> <numThreads>");
            System.exit(1);
        }

        String brokers = args[0];
        String topic = args[1];
        int num = Integer.parseInt(args[2]);
        int numThreads = Integer.parseInt(args[3]);

        ExecutorService executor = Executors.newCachedThreadPool();
        for (int i = 0; i < numThreads; i++) {
            int offset = i * num + 1;
            executor.execute(new ImgProducerPHY(brokers, topic, offset, num));
        }
    }

    public ImgProducerPHY(String brokers, String topic, int offset, int num) {
        super(brokers, topic, offset, num);
    }

    @Override
    public String getImgPath(int id) {
        String imgFilename = getImgFilename(id);
        int diskId = id % 7 + 1;
        return "/mnt/DP_disk" + diskId + "/jiacheng/testdata/" + imgFilename;
    }
}
