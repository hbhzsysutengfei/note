import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class DataWritable implements Writable {

	// up load
	private int upPackNum;
	private int upPayLoad;

	// down load
	private int downPackNum;
	private int downPayLoad;

	public DataWritable() {

	}

	public DataWritable(int upPackNum, int upPayLoad, int downPackNum,
			int downPayLoad) {
		this.set(upPackNum, upPayLoad, downPackNum, downPayLoad);
	}

	public void set(int upPackNum, int upPayLoad, int downPackNum,
			int downPayLoad) {
		this.upPackNum = upPackNum;
		this.upPayLoad = upPayLoad;
		this.downPackNum = downPackNum;
		this.downPayLoad = downPayLoad;
	}

	public int getUpPackNum() {
		return upPackNum;
	}

	public int getUpPayLoad() {
		return upPayLoad;
	}

	public int getDownPackNum() {
		return downPackNum;
	}

	public int getDownPayLoad() {
		return downPayLoad;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(upPackNum);
		out.writeInt(upPayLoad);
		out.writeInt(downPackNum);
		out.writeInt(downPayLoad);
	}

	public void readFields(DataInput in) throws IOException {
		this.upPackNum = in.readInt();
		this.upPayLoad = in.readInt();
		this.downPackNum = in.readInt();
		this.downPayLoad = in.readInt();
	}

	@Override
	public String toString() {
		return upPackNum + "\t" + upPayLoad + "\t" + downPackNum + "\t"
				+ downPayLoad;
	}
}
