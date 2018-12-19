package bigdata.utils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;

public class dataspilt {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
			InputStream is = new FileInputStream("E:\\NginxData.txt");
			BufferedReader reader = new BufferedReader(new InputStreamReader(is));
			String str = null;
			while (true) {
				str = reader.readLine();

				String[] split = str.split("\t");

				System.out.println(split.length);

				for(int i = 0 ; i<split.length ; i++ ){
					System.out.println("第"+i+"个数为： "+split[i]);
				}


				if(str!=null)
					System.out.println(str);
				else
					break;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}


	}

}
