package join;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;


public class SortPartitioner extends Partitioner<IntWritable, Text> {

	@Override
	public int getPartition(IntWritable key, Text value, int numPartition) {
		
		//int keyInt = key.get();
		int keyInt = key.get();
		if (keyInt < 100) 
			return 0;
		if (keyInt < 2050 )
			return 1;
		else
			return 2;
	}
		
}

