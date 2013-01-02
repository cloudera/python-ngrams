import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class TextTriple implements WritableComparable<TextTriple> {
	
	private Text first;
	private Text second;
	private Text third;
	
	public TextTriple() {
		set(new Text(), new Text(), new Text());
	}
	
	public TextTriple(List<String> list) {
		set(new Text(list.get(0)),
			new Text(list.get(1)),
			new Text(list.get(2)));
	}
	
	public void set(Text first, Text second, Text third) {
		this.first = first;
		this.second = second;
		this.third = third;
	}
	
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
		third.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
		third.readFields(in);
	}

	@Override
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode() * 31 + third.hashCode();
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TextTriple) {
			TextTriple tt = (TextTriple) obj;
			return first.equals(tt.first) && second.equals(tt.second) && third.equals(tt.third);
		}
		return false;
	}
	
	@Override
	public String toString() {
		return first + "\t" + second + "\t" + third;
	}

	public int compareTo(TextTriple other) {
		int comp = first.compareTo(other.first);
		if (comp != 0) {
			return comp;
		}
		comp = second.compareTo(other.second);
		if (comp != 0) {
			return comp;
		}
		return third.compareTo(other.third);
	}
	
	
}
