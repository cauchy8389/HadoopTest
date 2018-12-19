package bigdata.utils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import jodd.typeconverter.Convert;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.hbase.util.Bytes;

public class utils {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.out.println("11".compareTo("1"));

//		if(Long.parseLong("0")>Long.MIN_VALUE){
//			System.out.println(1111);
//		}else{
//			System.out.println(222);
//		}



		int num = 64;
		System.out.println(Base64.encodeBase64String(Bytes.toBytes(num)));
		System.out.println(Base64.encodeBase64String(intToBytes(num)));

		System.out.println(Integer.toBinaryString(num));
		System.out.println(Integer.toHexString(num));

		//Byte.MAX_VALUE

		System.out.println(Hex.encodeHexString(Bytes.toBytes(num)));

		System.out.println(Bytes.toInt(Bytes.toBytes(num)));
		System.out.println(bytesToInt(intToBytes(num) , 0) );


		System.out.println(Bytes2ToString(StringToBytes("wo shi zhongguoren ")));


		System.out.println(Base64.encodeBase64String(intToBytes(num)));

		try {
			Date date = DateUtils.parseDate("08/Feb/2017:00:11:35", Locale.US,"dd/MMM/yyyy:HH:mm:ss");

			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			//Date date = simpleDateFormat.parse("08/Feb/2017:00:11:35");
			date = DateUtils.addDays(date, 1);

			//SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			System.out.println(simpleDateFormat.format(date));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		System.out.println();


		String slog = "www.sohu.com 222.241.203.10 - - [08/Feb/2017:00:07:44 +0800] 'POST /column/get_news_flash HTTP/1.1' 'http://www.sohu.com/column/get_news_flash' 200 56143 'http://www.sohu.com/' 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.2; WOW64; Trident/7.0; .NET4.0E; .NET4.0C; Tablet PC 2.0; .NET CLR 3.5.30729; .NET CLR 2.0.50727; .NET CLR 3.0.30729; InfoPath.2; LCJB)' '-' 'China' '11' 'Changsha'";

		String[] slogs = slog.split("\\s");

		for(String s : slogs){
			System.out.println(s);
		}

		try {
			Date date = DateUtils.parseDate(slogs[4].substring(1, 21), Locale.US,"dd/MMM/yyyy:HH:mm:ss");

			System.out.println(DateFormatUtils.ISO_DATE_FORMAT.format(date));

		} catch (ParseException e) {
			e.printStackTrace();
		}

	}

	public static byte[] StringToBytes(String value){

		try {
			return value.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static String Bytes2ToString(byte[] value){

		try {
			return new String(value,"UTF-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}


//	//为UTF8编码
//	byte[] isoret = srt2.getBytes("ISO-8859-1");
//	//为ISO-8859-1编码
//	其中ISO-8859-1为单字节的编码
//	2.byte[]转string
//	String isoString = new String(bytes,"ISO-8859-1");


	public static byte[] intToBytes(int value)
	{
		byte[] byte_src = new byte[4];
		byte_src[3] = (byte) ((value & 0xFF000000)>>24);
		byte_src[2] = (byte) ((value & 0x00FF0000)>>16);
		byte_src[1] = (byte) ((value & 0x0000FF00)>>8);
		byte_src[0] = (byte) ((value & 0x000000FF));
		return byte_src;
	}

	public static int bytesToInt(byte[] ary, int offset) {
		int value;
		value = (int) ((ary[offset]&0xFF)
				| ((ary[offset+1]<<8) & 0xFF00)
				| ((ary[offset+2]<<16)& 0xFF0000)
				| ((ary[offset+3]<<24) & 0xFF000000));
		return value;
	}

}
