package bigdata.mr.partition01;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Bean implements Writable {

	private String timestamp;
	private String ip;

	// 因为反序列化时需要反射，必须有一个无参构造函数，在这里显示定义一个
	public Bean() {
	}

	// 定义了一个有参构造函数，会覆盖掉默认的无参构造函数
	public Bean(String timestamp, String ip) {
		this.set(timestamp, ip);
	}

	public void set(String timestamp, String ip) {
		this.timestamp = timestamp;
		this.ip = ip;
	}

	// 序列化方法
	public void write(DataOutput out) throws IOException {
		out.writeUTF(timestamp);
		out.writeUTF(ip);
	}

	// 反序列化方法 ----序列化和反序列化时数据字段的顺序要一致
	public void readFields(DataInput in) throws IOException {
		timestamp = in.readUTF();
		ip = in.readUTF();
	}

	public String toString() {

		return timestamp + "\t" + ip;
	}

}
