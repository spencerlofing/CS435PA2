import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IDFReducer extends Reducer<Text, Text, Text, DoubleWritable>{
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
	{
		double authorCount = 1;
		ArrayList<String> cache = new ArrayList<String>();
		Iterator<Text> it = values.iterator();
		String[] authorParse;
		while(it.hasNext())
		{
			String val = it.next().toString();
			String[] parse = val.split("\t");
			String[] authors = parse[0].split("\\[");
			if(authors[0].equals("Author Count: "))
			{
				authorCount = Double.parseDouble(parse[1]); 
				authorParse = parse[0].split(", ");
			}
			else
			{
				cache.add(val);
			}
		}
		for(String val : cache)
		{
			
			String[] parse = val.split("\t");
			Double DTF = Double.parseDouble(parse[1]);
			Double IDF = Math.log10(DTF/authorCount);
			context.write(new Text(parse[0]), new DoubleWritable(IDF));
		}
	}

}
