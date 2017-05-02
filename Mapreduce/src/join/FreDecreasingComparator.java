package join;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;

public class FreDecreasingComparator extends WritableComparator {

	protected FreDecreasingComparator() {
		super(Text.class,true);
	}

	
	public int compare(WritableComparable w1, WritableComparable w2) {
		
		String tip1 = w1.toString();
		String tip2 = w2.toString();
		String[] dataArray1 = tip1.toString().split("\t");
		String[] dataArray2 = tip2.toString().split("\t");
		
		int v1 = Integer.parseInt(dataArray1[1]);
		int v2 = Integer.parseInt(dataArray2[1]);
		if (v1 < v2)
		{
			return 1;
		}
		else
		{
			return -1;
		}

		
	}
	
}

