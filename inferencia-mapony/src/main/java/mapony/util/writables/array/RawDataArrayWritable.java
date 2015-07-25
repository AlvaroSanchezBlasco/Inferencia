package mapony.util.writables.array;

import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

import mapony.util.writables.RawDataWritable;

/**
 * @author Alvaro Sanchez Blasco
 *         <p>
 *         Implementacion de un CustomArrayWritable que contenga el CustomWritable que hemos creado para este proyecto.
 *         <p>
 *         Implementation of a Custom ArrayWritable to contain the CustomWritable thah we've created in this project.
 */
public class RawDataArrayWritable extends ArrayWritable {

	/**
	 * Default constructor
	 */
	public RawDataArrayWritable() {
		super(RawDataWritable.class);
	}

	/**
	 * @param values
	 */
	public RawDataArrayWritable(RawDataWritable[] values) {
		super(RawDataWritable.class);
	}

	/**
	 * @param valueClass
	 * @param values
	 */
	public RawDataArrayWritable(Class<? extends Writable> valueClass, RawDataWritable[] values) {
		super(valueClass, values);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.ArrayWritable#get()
	 */
	public Writable[] get() {
		return (Writable[]) super.get();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return Arrays.toString(get());
	}

}
