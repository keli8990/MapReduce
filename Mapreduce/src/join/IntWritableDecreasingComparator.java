package join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

public class IntWritableDecreasingComparator extends WritableComparator 
{
	protected IntWritableDecreasingComparator() 
	{
		super(IntWritable.class,true);
	}

	public int compare(WritableComparable w1, WritableComparable w2) 
	{		
		int tip1 = Integer.parseInt(w1.toString());
		int tip2 = Integer.parseInt(w2.toString());
		if (tip1 < tip2)
		{
			return 1;
		}
		else
		{
			return -1;
		}

	}
	
}

