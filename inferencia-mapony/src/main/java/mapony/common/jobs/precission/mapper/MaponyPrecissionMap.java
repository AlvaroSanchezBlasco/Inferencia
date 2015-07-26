package mapony.common.jobs.precission.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapony.util.clases.MaponyUtil;
import mapony.util.constantes.MaponyPropertiesCte;
import mapony.util.writables.RawDataWritable;
import mapony.util.writables.array.RawDataArrayWritable;

/**
 * @author Alvaro Sanchez Blasco
 *         <p>
 *         Mapper del job de agrupacion de mayor precision
 *         <p>
 *         Ya no vamos a centrar el calculo del geoHash en una ciudad. Ahora, lo que haremos sera agrupar los
 *         datos segun la cercania entre ellos. Por eso recalculamos un nuevo geoHash de mayor precision (menor
 *         area) y emitimos los datos, para agruparlos.
 */
public class MaponyPrecissionMap extends Mapper<Text, RawDataArrayWritable, Text, RawDataWritable> {

	private static final Logger logger = LoggerFactory.getLogger(MaponyPrecissionMap.class);
	private int precisionGeoHash;

	protected void map(Text key, RawDataArrayWritable values, Context context)
			throws IOException, InterruptedException {

		try {
			Writable[] tmp = values.get();
			for (Writable writable : tmp) {
				RawDataWritable line = new RawDataWritable((RawDataWritable) writable);

				final Text longitud = new Text(line.getLongitude().toString());
				final Text latitud = new Text(line.getLatitude().toString());

				final Text geoHashCalculado = MaponyUtil.getGeoHashPorPrecision(longitud, latitud, precisionGeoHash);

				RawDataWritable rdBean = new RawDataWritable(new Text(line.getIdentifier().toString()),
						new Text(line.getDateTaken().toString()), new Text(line.getCaptureDevice().toString()),
						new Text(line.getTitle().toString()), new Text(line.getDescription().toString()),
						new Text(line.getUserTags().toString()), new Text(line.getMachineTags().toString()),
						new Text(line.getLongitude().toString()), new Text(line.getLatitude().toString()),
						new Text(line.getDownloadUrl().toString()), new Text(geoHashCalculado.toString()),
						new Text(line.getCiudad().toString()));

				context.write(new Text(geoHashCalculado.toString()), new RawDataWritable(rdBean));
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	public void setup(Context context) {
		precisionGeoHash = Integer.parseInt(context.getConfiguration().get(MaponyPropertiesCte.precision));
	}
}
