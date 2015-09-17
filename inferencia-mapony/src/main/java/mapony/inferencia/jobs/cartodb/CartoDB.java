package mapony.inferencia.jobs.cartodb;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.LoggerFactory;

import mapony.inferencia.combiner.CommonCombiner;
import mapony.inferencia.jobs.InferenciaCustomJob;
import mapony.inferencia.mapper.cartoDb.CartoDbMap;
import mapony.inferencia.partitioner.CityPartitioner;
import mapony.inferencia.reducer.CartoDbReducer;
import mapony.inferencia.util.cte.InferenciaCte;
import mapony.inferencia.util.cte.JobNamesCte;
import mapony.inferencia.util.cte.PropertiesCte;
import mapony.inferencia.util.exception.InferenciaException;
import mapony.inferencia.writables.RawData;

public class CartoDB extends InferenciaCustomJob {

	public static void main(final String args[]) throws Exception {
		checkMainClassArgs(args);
		System.exit(ToolRunner.run(new CartoDB(), args));
	}

	@Override
	public Job createJobAndSetJarByClass(Configuration config) throws InferenciaException {
		try {
			final Job job = Job.getInstance(config, getJobName());
			job.setJarByClass(CartoDB.class);
			return job;
		} catch (IOException e) {
			throw new InferenciaException(e, e.getMessage());
		}
	}

	@Override
	public void setClassLogger() {
		logger = LoggerFactory.getLogger(CartoDB.class);
	}

	protected void init(Configuration config) {
	}

	@Override
	protected void init() {
		setJobName(JobNamesCte.generateCsv);

		setIndice_archivos(properties.getProperty(PropertiesCte.indice_archivo));
		setPatronFicheros(properties.getProperty(PropertiesCte.patron_ficheros));

		// Completamos la ruta con la mascara de ficheros a buscar
		setRutaFicheros(properties.getProperty(PropertiesCte.datos_iniciales) + getIndice_archivos() + getPatronFicheros());

		// Anyadimos el indice a la salida del job para diferenciar los datos de cada job con el archivo original
		setRutaSalidaFicheros(properties.getProperty(PropertiesCte.salida_datos_job) + getIndice_archivos());
		setNumReducers(Integer.parseInt(properties.getProperty(PropertiesCte.reducers)));
	}

	@Override
	protected void setJobOutputFormat(Job job) {
		// La salida de nuestro Job sera del tipo Text.
		job.setOutputFormatClass(TextOutputFormat.class);
	}

	@Override
	protected void setMappperOutput(Job job) {
		// Salida del Mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(RawData.class);
	}

	@Override
	protected void setReducerOutput(Job job) {
		// Salida del Job
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
	}

	@Override
	protected int setJobInputData(Configuration config, Job job) throws InferenciaException {
		try {
			// Recuperamos los ficheros que vamos a procesar, y los anhadimos como datos de entrada
			final FileSystem fs = FileSystem.get(new URI(InferenciaCte.hdfsUri), config);

			// Recuperamos los datos a procesar del path origen (out/part-r-*)
			FileStatus[] glob = fs.globStatus(new Path(getRutaFicheros()));

			// Si tenemos datos...
			if (null != glob) {
				if (glob.length > 0) {
					for (FileStatus fileStatus : glob) {
						Path pFich = fileStatus.getPath();
						// MultipleInputs
						MultipleInputs.addInputPath(job, pFich, SequenceFileInputFormat.class,
								CartoDbMap.class);
					}
				}
			} else {
				return noDataFound();
			}
		} catch (IOException e) {
			throw new InferenciaException(e, e.getMessage());
		} catch (URISyntaxException e) {
			throw new InferenciaException(e, e.getMessage());
		}
		return InferenciaCte.SUCCESS;
	}

	protected void setJobClasses(Job job) {
		// Combiner de nuestro Job
		job.setCombinerClass(CommonCombiner.class);

		// Reducer de nuestro Job
		job.setReducerClass(CartoDbReducer.class);
		
		
		job.setPartitionerClass(CityPartitioner.class);

	}
}
