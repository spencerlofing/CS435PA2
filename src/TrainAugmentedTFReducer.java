import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TrainAugmentedTFReducer extends Reducer<Text, WordCount, Text, DoubleWritable>{
	public void reduce(Text key, Iterable<WordCount> values, Context context) throws
	IOException, InterruptedException {
		double max = 0;
		Iterator<WordCount> it = values.iterator();
		ArrayList<String> cache = new ArrayList<String>();
		while(it.hasNext())
		{
			WordCount wc = it.next();
			cache.add(wc.toString());
			if(wc.getCount() > max)
			{
				max = wc.getCount();
			}
			//context.write(new Text(key + " " + wc.getWord()), new DoubleWritable(wc.getCount()));
		}
		for(int i = 0; i < cache.size(); i++)
		{
			String wc = cache.get(i);
			String[] parse = wc.split("\t");
			String newKey = key + "\t" + parse[0];
			double augmentedTF = 0.5 + 0.5*(Double.parseDouble(parse[1])/max);
			//context.write(new Text("here"), new DoubleWritable(1.3));
			context.write(new Text(newKey), new DoubleWritable(augmentedTF));
		}
	}
}
