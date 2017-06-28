import java.io.*;
import java.util.*;
import java.util.regex.*;

public class Calc {
    public static void main(String[] args) throws IOException{
        String pattern="receive in \\d+ms";
        Pattern r=Pattern.compile(pattern);
        Matcher matcher;
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
        int total=results.size();
        Long avg=sum/total;
        System.out.println("process log from file "+f.getName());
        System.out.println("total: "+total+" min: "+min+"ms max: "+max+"ms avg: "+avg+"ms");
    }
}
