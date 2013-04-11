package com.renren.tailor.exec;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ExecReducer extends
		Reducer<LongWritable, Text, NullWritable, Text> {

	@Override
	protected void reduce(
			LongWritable arg0,
			Iterable<Text> values,
			org.apache.hadoop.mapreduce.Reducer<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		Iterator<Text> it = values.iterator();
		Text t;
		while (it.hasNext()) {
			t = it.next();
			try {
				context.write(NullWritable.get(), t);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
