package mapony.inferencia.mapper.cartoDb;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import mapony.inferencia.writables.RawData;
import mapony.inferencia.writables.arraywritables.ArrayOfRawData;

public class CartoDbMap extends Mapper<Text, ArrayOfRawData, Text, RawData> {

	protected void map(Text key, ArrayOfRawData values, Context context)
			throws IOException, InterruptedException {

		Writable[] arrayOfWritable = values.get();
		// Sabemos que los datos recuperados vienen del anterior Job, por lo
		// que son validos. No tenemos que volver a validarlos para
		// procesarlos.
		if(arrayOfWritable.length >= 20){
			for (Writable writable : arrayOfWritable) {
				RawData processedRecord = new RawData((RawData) writable);
				context.write(new Text(processedRecord.getIdentifier()), new RawData(processedRecord));
			}
		}
	}
}
