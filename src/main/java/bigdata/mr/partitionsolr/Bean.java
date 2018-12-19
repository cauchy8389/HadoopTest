package bigdata.mr.partitionsolr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Bean implements WritableComparable<Bean> {

	private String domain;
	private long Width;

	// 因为反序列化时需要反射，必须有一个无参构造函数，在这里显示定义一个
	public Bean() {
	}

	// 定义了一个有参构造函数，会覆盖掉默认的无参构造函数
	public Bean(String domain, long Width) {
		this.set(domain, Width);
	}

	public void set(String domain, long Width) {
		this.domain = domain;
		this.Width = Width;
	}

	public String getDomain() {
		return domain;
	}

	public long getWidth() {
		return Width;
	}

	// 序列化方法
	public void write(DataOutput out) throws IOException {
		out.writeUTF(domain);
		out.writeLong(Width);

	}

	// 反序列化方法 ----序列化和反序列化时数据字段的顺序要一致
	public void readFields(DataInput in) throws IOException {
		domain = in.readUTF();
		Width = in.readLong();
	}

	public String toString() {

		return domain + "\t" + Width;
	}

	public int compareTo(Bean Key) {

		return (int) (Width - Key.Width);

	}
}
