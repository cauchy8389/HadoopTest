package bigdata.mr.secondsolr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Bean implements WritableComparable<Bean> {

	private long width;
	private long timestamp;

	// 因为反序列化时需要反射，必须有一个无参构造函数，在这里显示定义一个
	public Bean() {
	}

	// 定义了一个有参构造函数，会覆盖掉默认的无参构造函数
	public Bean(long width, long timestamp) {
		this.set(width, timestamp);
	}

	public void set(long width, long timestamp) {
		this.width = width;
		this.timestamp = timestamp;
	}

	public long getWidth() {
		return width;
	}

	// 序列化方法
	public void write(DataOutput out) throws IOException {
		out.writeLong(width);
		out.writeLong(timestamp);

	}

	// 反序列化方法 ----序列化和反序列化时数据字段的顺序要一致
	public void readFields(DataInput in) throws IOException {
		width = in.readLong();
		timestamp = in.readLong();
	}

	public int compareTo(Bean Key) {
		long min = width - Key.width;
		if (min != 0) {
			// 说明第一列不相等，则返回两数之间小的数
			return (int) min;
		} else {
			return (int) (timestamp - Key.timestamp);
		}
	}
}
