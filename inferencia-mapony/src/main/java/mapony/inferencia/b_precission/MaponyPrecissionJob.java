package mapony.inferencia.b_precission;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapony.common.combiner.MaponyCombiner;
import mapony.common.jobs.precission.mapper.MaponyPrecissionMap;
import mapony.common.reducer.total.MaponyReducer;
import mapony.util.constantes.MaponyCte;
import mapony.util.constantes.MaponyPropertiesCte;
import mapony.util.writables.RawDataWritable;
import mapony.util.writables.array.RawDataArrayWritable;

/**
 * @author Alvaro Sanchez Blasco
 *         <p>
 *         Job para la agrupacion de los datos a procesar. Leemos los datos de la ruta especificada en el properties
 *         (ruta_ficheros).
 *         <p>
 *         Debemos tener en hdfs, en la ruta especificada en el properties (ruta_paises) el fichero de ciudades obtenido
 *         de geonames (se encuentra en la carpeta ext_dataset) del proyecto.
 * 
 */
public class MaponyPrecissionJob extends Configured implements Tool {

	private static Properties properties;
	private static final Logger logger = LoggerFactory.getLogger(MaponyPrecissionJob.class);
	private String rutaFicheros;
	private int numReducers;
	private String rutaSalidaFicheros;
	private String indice_archivos;
	private String patronFicheros;

	private final String nombreJob = MaponyCte.jobNameMayorPrecision;

	/**
	 * Carga las propiedades para el Job
	 * 
	 * @param fileName
	 * @throws IOException
	 */
	private static void loadProperties(final String fileName) throws IOException {
		if (null == properties) {
			properties = new Properties();
		}
		try {
			FileInputStream in = new FileInputStream(fileName);
			properties.load(in);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Inicializa los parametros necesarios del job obtenidos de las propiedades, y anyade los datos que haran falta en
	 * el Mapper o el Reducer al objeto Configuration que le llega como parametro
	 * 
	 * @param config
	 */
	private void init(Configuration config) {
		setIndice_archivos(properties.getProperty(MaponyPropertiesCte.indice_archivo));
		setPatronFicheros(properties.getProperty(MaponyPropertiesCte.patron_ficheros));
		//Completamos la ruta con la mascara de ficheros a buscar
		setRutaFicheros(properties.getProperty(MaponyPropertiesCte.datos_iniciales)+getIndice_archivos()+getPatronFicheros());
		//Anyadimos el indice a la salida del job para diferenciar los datos de cada job con el archivo original
		setRutaSalidaFicheros(properties.getProperty(MaponyPropertiesCte.salida_datos_job)+getIndice_archivos());
		setNumReducers(Integer.parseInt(properties.getProperty(MaponyPropertiesCte.reducers)));

		config.set(MaponyPropertiesCte.precision, properties.getProperty(MaponyPropertiesCte.precision));
		config.set(MaponyPropertiesCte.reservoir, properties.getProperty(MaponyPropertiesCte.reservoir));
	}
	
	public int run(String[] args) throws Exception {

		Configuration config = getConf();

		init(config);
		
		Job job = Job.getInstance(config, nombreJob);
		job.setJarByClass(MaponyPrecissionJob.class);

		// La salida de nuestro Job sera del tipo Sequencefile con compresion a nivel de Bloque.
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

		// Salida del Mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(RawDataWritable.class);

		// Salida del Job
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(RawDataArrayWritable.class);

		// Recuperamos los ficheros que vamos a procesar, y los anhadimos como datos de entrada
		final FileSystem fs = FileSystem.get(new URI("hdfs://quickstart.cloudera:8020/"), config);

		// Path de destino de ejecucion
		Path outPath = new Path(getRutaSalidaFicheros());

		// Borramos todos los directorios que puedan existir
		FileSystem.get(outPath.toUri(), config).delete(outPath, true);

		// Recuperamos los datos a procesar del path origen (out/part-r-*)
		try {
			FileStatus[] glob = fs.globStatus(new Path(getRutaFicheros()));

			// Si tenemos datos...
			if (null != glob) {
				if (glob.length > 0) {
					for (FileStatus fileStatus : glob) {
						Path pFich = fileStatus.getPath();
						// MultipleInputs
						MultipleInputs.addInputPath(job, pFich, SequenceFileInputFormat.class,
								MaponyPrecissionMap.class);
					}
				}
			} else {
				logger.error(MaponyCte.MSG_NO_DATOS + " '" + getRutaFicheros() + "'");
				return -1;
			}
		} catch (IOException e) {
			logger.error(MaponyCte.MSG_NO_DATOS + " '" + getRutaFicheros() + "'");
			return -1;
		}

		// Combiner de nuestro Job
		job.setCombinerClass(MaponyCombiner.class);

		// Reducer de nuestro Job
		job.setReducerClass(MaponyReducer.class);

		job.setNumReduceTasks(getNumReducers());

		FileOutputFormat.setOutputPath(job, outPath);

		boolean success = job.waitForCompletion(true);
		logger.info(MaponyCte.getMsgFinJob(nombreJob));
		return (success ? 0 : 1);
	}

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception {
		if (args.length != 1) {
			System.out.printf("Uso: <config.properties file>\n");
			System.exit(-1);
		}
		loadProperties(args[0]);

		getLogger().info(MaponyPropertiesCte.MSG_PROPIEDADES_CARGADAS);

		ToolRunner.run(new MaponyPrecissionJob(), args);
		System.exit(1);

	}

	/**
	 * @return the logger
	 */
	private static final Logger getLogger() {
		return logger;
	}

	/**
	 * @return the rutaFicheros
	 */
	private final String getRutaFicheros() {
		return rutaFicheros;
	}

	/**
	 * @param rutaFicheros
	 *            the rutaFicheros to set
	 */
	private final void setRutaFicheros(String rutaFicheros) {
		this.rutaFicheros = rutaFicheros;
	}

	/**
	 * @return the numReducers
	 */
	private final int getNumReducers() {
		return numReducers;
	}

	/**
	 * @param numReducers
	 *            the numReducers to set
	 */
	private final void setNumReducers(int numReducers) {
		this.numReducers = numReducers;
	}

	/**
	 * @return the rutaSalidaFicheros
	 */
	private final String getRutaSalidaFicheros() {
		return rutaSalidaFicheros;
	}

	/**
	 * @param rutaSalidaFicheros the rutaSalidaFicheros to set
	 */
	private final void setRutaSalidaFicheros(String rutaSalidaFicheros) {
		this.rutaSalidaFicheros = rutaSalidaFicheros;
	}

	/**
	 * @return the indice_archivos
	 */
	private final String getIndice_archivos() {
		return indice_archivos;
	}

	/**
	 * @param indice_archivos the indice_archivos to set
	 */
	private final void setIndice_archivos(String indice_archivos) {
		this.indice_archivos = indice_archivos;
	}

	/**
	 * @return the patronFicheros
	 */
	private final String getPatronFicheros() {
		return patronFicheros;
	}

	/**
	 * @param patronFicheros the patronFicheros to set
	 */
	private final void setPatronFicheros(String patronFicheros) {
		this.patronFicheros = patronFicheros;
	}
}
