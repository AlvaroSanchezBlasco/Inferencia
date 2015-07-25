package mapony.inferencia.a_groupNear.total;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapony.inferencia.a_groupNear.total.mapper.MaponyGroupNearMap;
import mapony.inferencia.common.combiner.MaponyCombiner;
import mapony.inferencia.common.reducer.MaponyReducer;
import mapony.util.clases.GeoHashCiudad;
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
 */
public class MaponyGroupNearJob extends Configured implements Tool {

	private static Properties properties;
	private static final Logger logger = LoggerFactory.getLogger(MaponyGroupNearJob.class);
	private String rutaFicheros;
	private int numReducers;
	private int precisionGeoHash;
	private String indiceArchivo;
	private String rutaSalidaFicheros;
	private String rutaPaises;

	private final String nombreJob = MaponyCte.jobNameGroupNear;
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
		setIndiceArchivo(properties.getProperty(MaponyPropertiesCte.indice_archivo));
		setRutaFicheros(properties.getProperty(MaponyPropertiesCte.datos_iniciales) + getIndiceArchivo()
				+ MaponyPropertiesCte.ext_archivos);
		setNumReducers(Integer.parseInt(properties.getProperty(MaponyPropertiesCte.reducers)));
		setPrecisionGeoHash(Integer.parseInt(properties.getProperty(MaponyPropertiesCte.precision)));
		setRutaSalidaFicheros(properties.getProperty(MaponyPropertiesCte.salida_datos_job) + getIndiceArchivo());
		setRutaPaises(properties.getProperty(MaponyPropertiesCte.paises));

		config.set(MaponyPropertiesCte.precision, properties.getProperty(MaponyPropertiesCte.precision));
		config.set(MaponyPropertiesCte.indice_archivo, properties.getProperty(MaponyPropertiesCte.indice_archivo));
		config.set(MaponyPropertiesCte.reservoir, properties.getProperty(MaponyPropertiesCte.reservoir));
	}

	public int run(String[] args) throws Exception {
		Configuration config = getConf();

		init(config);

		Job job = Job.getInstance(config, nombreJob);
		job.setJarByClass(MaponyGroupNearJob.class);

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

		// Recuperamos los ficheros que vamos a procesar, y los anyadimos como datos de entrada
		final FileSystem fs = FileSystem.get(new URI("hdfs://quickstart.cloudera:8020/"), config);

		// Path de destino de ejecución
		Path outPath = new Path(getRutaSalidaFicheros());

		// Borramos todos los directorios que puedan existir
		FileSystem.get(outPath.toUri(), config).delete(outPath, true);

		// Recuperamos el contenido del fichero de países y ciudades obtenido de geonames, para cargarlos en memoria.
		Path pathPaises = new Path(getRutaPaises());

		FileSystem hdfs = FileSystem.get(config);
		String linea = null;
		BufferedReader d = new BufferedReader(new InputStreamReader(hdfs.open(pathPaises)));
		while ((linea = d.readLine()) != null) {
			GeoHashCiudad.cargaHashDatosCiudades(linea, getPrecisionGeoHash());
		}

		// Recuperamos los datos a procesar del path origen (data/*.bz2)
		try {
			FileStatus[] glob = fs.globStatus(new Path(getRutaFicheros()));

			// Si tenemos datos...
			if (null != glob) {
				if (glob.length > 0) {
					for (FileStatus fileStatus : glob) {
						Path pFich = fileStatus.getPath();
						// MultipleInputs
						MultipleInputs.addInputPath(job, pFich, TextInputFormat.class, MaponyGroupNearMap.class);
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

		logger.info(MaponyPropertiesCte.MSG_PROPIEDADES_CARGADAS);

		ToolRunner.run(new MaponyGroupNearJob(), args);
		System.exit(1);

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
	 * @return the precisionGeoHash
	 */
	private final int getPrecisionGeoHash() {
		return precisionGeoHash;
	}

	/**
	 * @param precisionGeoHash
	 *            the precisionGeoHash to set
	 */
	private final void setPrecisionGeoHash(int precisionGeoHash) {
		this.precisionGeoHash = precisionGeoHash;
	}

	/**
	 * @return the indiceArchivo
	 */
	private final String getIndiceArchivo() {
		return indiceArchivo;
	}

	/**
	 * @param indiceArchivo
	 *            the indiceArchivo to set
	 */
	private final void setIndiceArchivo(String indiceArchivo) {
		this.indiceArchivo = indiceArchivo;
	}

	/**
	 * @return the rutaSalidaFicheros
	 */
	private final String getRutaSalidaFicheros() {
		return rutaSalidaFicheros;
	}

	/**
	 * @param rutaSalidaFicheros
	 *            the rutaSalidaFicheros to set
	 */
	private final void setRutaSalidaFicheros(String rutaSalidaFicheros) {
		this.rutaSalidaFicheros = rutaSalidaFicheros;
	}

	/**
	 * @return the rutaPaises
	 */
	private final String getRutaPaises() {
		return rutaPaises;
	}

	/**
	 * @param rutaPaises
	 *            the rutaPaises to set
	 */
	private final void setRutaPaises(String rutaPaises) {
		this.rutaPaises = rutaPaises;
	}
}
