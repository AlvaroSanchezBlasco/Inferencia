// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.writables.arraywritables;

import java.util.Arrays;

import mapony.inferencia.writables.RawData;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

/**
 * @author Alvaro Sanchez Blasco
 *         <p>
 *         Implementacion de un CustomArrayWritable que contenga el CustomWritable que hemos creado para este proyecto.
 *         <p>
 *         Implementation of a Custom ArrayWritable to contain the CustomWritable thah we've created in this project.
 */
public class ArrayOfRawData extends ArrayWritable {

	public ArrayOfRawData() {
		super(RawData.class);
	}

	public ArrayOfRawData(RawData[] values) {
		super(RawData.class);
	}

	public ArrayOfRawData(Class<? extends Writable> valueClass, RawData[] values) {
		super(valueClass, values);
	}

	public Writable[] get() {
		return (Writable[]) super.get();
	}

	public String toString() {
		return Arrays.toString(get());
	}

}
