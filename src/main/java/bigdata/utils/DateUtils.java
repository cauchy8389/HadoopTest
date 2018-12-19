package bigdata.utils;


import java.text.SimpleDateFormat;
import java.util.Date;


public class DateUtils {

    public String stampToDate(String s){
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long lt = new Long(s);
        Date date = new Date(lt);
        res = simpleDateFormat.format(date);
        return res;
    }

    public  void main(String[] args ){


        String log = "1495794792.178125.122.210.19664d02d6b1530b6ff1172b1ee878e7096fa11d1b492f1cdf4a7f9b88dbaa706008.wybi.net144090032zh-CNhttp://8.wybi.net/Promotion/videoroom_per_batchstatic.aspx?batch=52http://8.wybi.net/Promotion/videoroom_per_batchstatic.aspx?batch=52Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36mawenbo_trackClick648|558|nullX1495794792.178125.122.210.19664d02d6b1530b6ff1172b1ee878e7096fa11d1b492f1cdf4a7f9b88dbaa706008.wybi.net144090032zh-CNhttp://8.wybi.net/Promotion/videoroom_per_batchstatic.aspx?batch=52http://8.wybi.net/Promotion/videoroom_per_batchstatic.aspx?batch=52Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.63 Safari/537.36mawenbo_trackClick648|558|null";


        String  time1 = "1495796401072";
        String  time2 = "1495794797512";

        System.out.println(stampToDate(time1));
        System.out.println(stampToDate(time2));


    }

}
