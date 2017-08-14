import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class WordCount implements WritableComparable<WordCount>{

	private String word;
	private Double count;
	
	public WordCount() { }
	
	public WordCount(String word, Double count)
	{
		this.word = word;
		this.count = count;
	}
	
	public String toString()
	{
		return(new StringBuilder())
				.append(word)
				.append('\t')
				.append(count)
				.toString();
	}
	
	public void readFields(DataInput in) throws IOException {
		word = WritableUtils.readString(in);
		count = in.readDouble();	
	}
	
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, word);
		out.writeDouble(count);
	}
	
	public int compareTo(WordCount wc) {
		int result = word.compareTo(wc.word);
		if(result == 0)
		{
			result = count.compareTo(wc.count);
		}
		return result;
	}
	
	public String getWord() {
		return word;
	}
	
	public void setWord(String word) {
		this.word = word;
	}
	public Double getCount() {
		return count;
	}
	
	public void setCount(Double count) {
		this.count = count;
	}
}
