import java.io.*;
import java.util.*;
import java.util.regex.*;

public class Calc {
    public static void main(String[] args) throws IOException{
        String pattern="receive in \\d+ms";
        Pattern r=Pattern.compile(pattern);
        Matcher matcher;
        String logFilename="/home/jiacheng/tmp-ocr/"+args[0];
        File f=new File(logFilename);
        InputStreamReader isr=new InputStreamReader(new FileInputStream(f));
        BufferedReader buf=new BufferedReader(isr);
        String line=null;
        List<String> results=new ArrayList<>();
        while((line=buf.readLine())!=null){
            matcher=r.matcher(line);
            if(matcher.find()) {
                String matchedLine = matcher.group(0);
                String latency=matchedLine.substring(11,matchedLine.length()-2);
                results.add(latency);
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