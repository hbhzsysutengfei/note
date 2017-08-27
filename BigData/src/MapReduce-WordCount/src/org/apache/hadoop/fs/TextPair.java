package org.apache.hadoop.fs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class TextPair implements WritableComparable<TextPair>{
	private Text first;
	private Text second;
	
	public TextPair(){
		
	}
	
	public  TextPair(Text first, Text second) {
		this.set(first, second);
	}

	public void set(Text first, Text second){
		this.first = first;
		this.second = second;
	}
	
	public Text getFirst(){
		return this.first;
	}
	
	public Text getSecond(){
		return this.second;
	}

	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		first.readFields(in);
		first.readFields(in);
	}

	public int compareTo(TextPair o) {
		// TODO Auto-generated method stub
		int cmp = this.first.compareTo(o.getFirst());
		if(0!= cmp){
			return cmp;
		}
		return this.second.compareTo(o.getSecond());
	}
	
	
	@Override
	public int hashCode() {
		
		return this.first.hashCode()*167+ this.second.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		if( obj instanceof TextPair){
			TextPair pair = (TextPair) obj;
			return first.equals(pair.getFirst()) && second.equals(pair.getSecond());
		}
		return false;
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return first + "\t" + second;
	}
	
	public static class  Comparator  extends WritableComparator{

		protected Comparator() {
			super(TextPair.class);
			// TODO Auto-generated constructor stub
		}
		
		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			// TODO Auto-generated method stub
			int n1 = WritableUtils.decodeVIntSize(b1[s1]);
			int n2 = WritableUtils.decodeVIntSize(b2[s2]);
			
//			int cmp = WritableComparator.compareBytes(b1, s1+n1, l1-s1, b2, s2+n2, l2-s2);
			
			return super.compare(b1, s1, l1, b2, s2, l2);
		}
		
	}
}
