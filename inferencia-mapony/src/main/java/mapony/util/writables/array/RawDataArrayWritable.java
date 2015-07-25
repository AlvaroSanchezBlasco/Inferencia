package mapony.util.writables.array;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

import mapony.util.writables.RawDataWritable;

public class RawDataArrayWritable extends ArrayWritable {

	public RawDataArrayWritable() {
		super(RawDataWritable.class);
	}

	public RawDataArrayWritable(RawDataWritable[] values) {
		super(RawDataWritable.class);
	}

	public RawDataArrayWritable(Class<? extends Writable> valueClass, RawDataWritable[] values) {
		super(valueClass, values);
	}
	
	public Writable[] get() {
		return (Writable[]) super.get();
	}

//	@Override
//	public void write(DataOutput arg0) throws IOException {
//	    for(RawDataWritable i : get()){
//	        i.write(arg0);
//	    }
//	}
	
	@Override
	public String toString() {
		return Arrays.toString(get());
	}
	
}
