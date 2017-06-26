import java.io.*;
import java.util.*;
import java.util.regex.*;

public class SummaryResults {
    public static void main(String[] args) throws IOException{
        String numRecordPerDisk=args[0];
        int num=Integer.parseInt(numRecordPerDisk);
        String pattern="finish in \\d+ms";
        Pattern r=Pattern.compile(pattern);
        Matcher matcher;
        List<String> results=new ArrayList<>();
        for(int i=1;i<=7;i++){
            String filename="/mnt/DP_disk"+i+"/jiacheng/ocr-tmp/output.txt";
            File f=new File(filename);
            InputStreamReader isr=new InputStreamReader(new FileInputStream(f));
            BufferedReader buf=new BufferedReader(isr);
            String line=null;
            List<String> resultLines=new ArrayList<>();
            while((line=buf.readLine())!=null){
                matcher=r.matcher(line);
                if(matcher.find()){
                    String matchedLine=matcher.group(0);
                    String latency=matchedLine.substring(10,matchedLine.length()-2);
                    resultLines.add(latency);
                }
            }
            int total=resultLines.size();
            for(int j=0;j<num;j++){
                results.add(resultLines.get(total-1-j));
            }

        }
        Long min=Long.parseLong(results.get(0));
        Long max=min;
        Long sum=0l;
        for(int i=0;i<results.size();i++){
            String lat=results.get(i);
            Long l=Long.parseLong(lat);
            sum+=l;
            if(l<min) min=l;
            if(l>max) max=l;
            System.out.println(lat);
        }
        Long avg=sum/results.size();
        System.out.println("Summary: min: "+min+"ms max: "+max+"ms avg: "+avg+"ms");
    }
}














