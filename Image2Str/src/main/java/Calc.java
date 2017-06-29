import com.sun.org.apache.xerces.internal.impl.xs.SchemaSymbols;

import java.io.*;
import java.util.*;
import java.util.regex.*;

public class Calc {
    public static void main(String[] args) throws IOException{
        String pattern="receive in \\d+ms";
        Pattern r=Pattern.compile(pattern);
        Matcher matcher;
        String pattern2="timestamp: \\d";
        Pattern r2=Pattern.compile(pattern2);
        Matcher matcher2;
        String logDir="/home/jiacheng/tmp-ocr";
        File dir=new File(logDir);
        File[] flist=dir.listFiles();
        int lastLog=0;
        for(int i=0;i<flist.length;i++){
            if (flist[i].isFile()){
                lastLog=i;
            }
        }
        File f=flist[lastLog];
        InputStreamReader isr=new InputStreamReader(new FileInputStream(f));
        BufferedReader buf=new BufferedReader(isr);
        String line=null;
        List<String> results=new ArrayList<>();
        List<String> times=new ArrayList<>();
        while((line=buf.readLine())!=null){
            matcher=r.matcher(line);
            if(matcher.find()) {
                String matchedLine = matcher.group(0);
                String latency=matchedLine.substring(11,matchedLine.length()-2);
                results.add(latency);
            }
            matcher2=r2.matcher(line);
            if(matcher2.find()){
                String matchedLine=matcher2.group(0);
                String timestamp=matchedLine.substring(11);

                times.add(timestamp);
            }
        }


        Long min=Long.parseLong(results.get(0));
        Long max=min;
        Long sum=0l;
        Long minTime=Long.parseLong(times.get(0));
        Long maxTime=minTime;
        int total=results.size();
        for(int i=0;i<total;i++){
            String lat=results.get(i);
            Long l=Long.parseLong(lat);
            sum+=l;
            if(l<min) min=l;
            if(l>max) max=l;
            Long lTime=Long.parseLong(times.get(i));
            if(lTime<minTime) minTime=lTime;
            if(lTime>maxTime) maxTime=lTime;
            System.out.println(lTime);
        }
        Long avg=sum/total;
        Long totalTime=(maxTime-minTime);
        double throughput=(double)total/totalTime*1000.0;
        System.out.println("process log from file "+f.getName());
        System.out.println("delay min="+min+"ms max="+max+"ms avg="+avg+"ms");
        System.out.println("complete "+total+" imgs in "+totalTime+"ms");
        System.out.printf("throughput=%.2fimg/s",throughput);

        isr.close();
    }
}






















