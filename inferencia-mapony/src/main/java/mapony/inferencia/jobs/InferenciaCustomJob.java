// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.jobs;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;

import mapony.inferencia.util.InferenciaMessages;
import mapony.inferencia.util.cte.InferenciaCte;
import mapony.inferencia.util.cte.PropertiesCte;
import mapony.inferencia.util.exception.InferenciaException;

/**
 * @author Alvaro Sanchez Blasco
 *         <ul>
 *         <li>[ES] Clase abstracta de la que heredaran los job del proceso de inferencia. Con la idea de realizar codigo limpio, se establecen
 *         metodos que se repiten en todas las ejecuciones de los Job.
 *         <li>[EN] Abstract class to use clean code. The configuration of a MapReduce Job is being cut into small methods.
 *         </ul>
 */
public abstract class InferenciaCustomJob extends Configured implements Tool {

	protected static Properties properties;
	protected static Logger logger;
	private String rutaFicheros;
	private int numReducers;
	private int precisionGeoHash;
	private String indiceArchivo;
	private String rutaSalidaFicheros;
	private String rutaPaises;
	private String jobName;
	private String indice_archivos;
	private String patronFicheros;
	
	protected static void checkMainClassArgs(final String args[]){
		if (args.length != InferenciaCte.NUM_ARGS) {
			System.out.println(PropertiesCte.usage);
			System.exit(InferenciaCte.FAIL);
		}
	}
	
	public int run(String[] args) throws InferenciaException, Exception {
		setClassLogger();

		loadProperties(args[0]);

		Configuration config = getConf();

		init(config);
		
		init();

		logger.info(InferenciaMessages.jobBegins(getJobName()));

		Job job = createJobAndSetJarByClass(config);

		setOutPuts(config, job);

		if (setJobInputData(config, job) == InferenciaCte.FAIL) {
			return evaluateEndJob(false);
		}

		setJobClasses(job);

		job.setNumReduceTasks(getNumReducers());

		boolean jobCompletion = false;

		try {
			jobCompletion = job.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			throw new InferenciaException(e, e.getMessage());
		} catch (IOException e) {
			throw new InferenciaException(e, e.getMessage());
		} catch (InterruptedException e) {
			throw new InferenciaException(e, e.getMessage());
		}

		return evaluateEndJob(jobCompletion);
	}

	public abstract Job createJobAndSetJarByClass(Configuration config) throws InferenciaException;
	
	public abstract void setClassLogger();
	
	public static int getJobExit(String args[], InferenciaCustomJob clase) throws InferenciaException{
		try {
			return ToolRunner.run(clase, args);
		} catch (Exception e) {
			throw new InferenciaException(e, e.getMessage());
		}
	}

	protected void loadProperties(final String fileName) throws InferenciaException {
		if (null == properties) {
			properties = new Properties();
		}
		try {
			FileInputStream in = new FileInputStream(fileName);
			properties.load(in);
		} catch (FileNotFoundException e) {
			throw new InferenciaException(e, e.getMessage());
		} catch (IOException e) {
			throw new InferenciaException(e, e.getMessage());
		}
		logger.info(PropertiesCte.PROPERTIES_LOADED);
	}

	/**
	 * <ul>
	 * <li>[ES] Inicializa los parametros necesarios del job obtenidos de las propiedades, y anyade los datos que haran falta en el Mapper o el
	 * Reducer al objeto Configuration que le llega como parametro
	 * <li>[EN] Initializes necessary params of the job, obtained of the properties. Adds parameters to the configuration that will be necessary in
	 * the Map or Reduce process.
	 * </ul>
	 * 
	 * @param config
	 */
	protected abstract void init(Configuration config);
	
	/**
	 * <ul>
	 * <li>[ES] Inicializa los parametros necesarios del job de carga obtenidos de las propiedades.
	 * <li>[EN] Initializes necessary params of the load job, obtained of the properties.
	 * </ul>
	 */
	protected abstract void init();

	/**
	 * Realiza el set de los output format del job, del mapper, del reducer y el directorio de salida de datos, de ser necesario.
	 * @throws InferenciaException 
	 * */
	protected void setOutPuts(Configuration config, Job job) throws InferenciaException {
		setJobOutputFormat(job);
		setMappperOutput(job);
		setReducerOutput(job);
		setJobOutputPath(config, job);
	}
	
	
	protected abstract void setJobOutputFormat(Job job);

	protected abstract void setMappperOutput(Job job);
	
	protected abstract void setReducerOutput(Job job);
	
	/**
	 * <ul>
	 * <li>[ES] Metodo para establecer el path donde guardar la salida del job
	 * <li>[EN] Method to set the output path of the Job
	 * </ul>
	 * 
	 * @param config
	 * @param job
	 * @throws InferenciaException 
	 */
	protected void setJobOutputPath(Configuration config, Job job) throws InferenciaException {
		// Path de destino de ejecucion
		Path outPath = new Path(getRutaSalidaFicheros());

		// Borramos todos los directorios que puedan existir
		try {
			FileSystem.get(outPath.toUri(), config).delete(outPath, true);
		} catch (IOException e) {
			throw new InferenciaException(e, e.getMessage());
		}
		FileOutputFormat.setOutputPath(job, outPath);	
	}
	
	/**
	 * <ul>
	 * <li>[ES] Metodo para establecer los input del Job.
	 * <li>[EN] Method to set the inputs of the Job.
	 * </ul>
	 * 
	 * @param config - the configuration of the job
	 * @param job - the job
	 * @return an integer. -1 if fail, 0 if success.
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	protected abstract int setJobInputData(Configuration config, Job job) throws InferenciaException;
	
	/**
	 * <ul>
	 * <li>[ES] Metodo para establecer las clases de Reduce, Combiner, Partitioner, etc. a nuestro Job.
	 * <li>[EN] Method to set the Reduce, Combiner, Partitioner, etc. classes to our Job.
	 * </ul>
	 * 
	 * @param job
	 */
	protected abstract void setJobClasses(Job job);
	
	protected int noDataFound(){
		logger.error(InferenciaMessages.noDataInSpecifiedPath(getRutaFicheros()));
		return InferenciaCte.FAIL;
	}
	
	protected int evaluateEndJob(boolean isSuccess) {
		if(isSuccess){
			logger.info(InferenciaMessages.jobEndedSuccessful(getJobName()));	
			return InferenciaCte.SUCCESS;
		} else {
			logger.info(InferenciaMessages.jobEndedNoCompletion(getJobName()));
			return InferenciaCte.JOB_COMPLETION_FAILED;
		}
	}
	
	protected final String getIndiceArchivo() {
		return indiceArchivo;
	}

	protected final void setIndiceArchivo(String indiceArchivo) {
		this.indiceArchivo = indiceArchivo;
	}

	protected final String getRutaFicheros() {
		return rutaFicheros;
	}

	protected final void setRutaFicheros(String rutaFicheros) {
		this.rutaFicheros = rutaFicheros;
	}

	protected final int getNumReducers() {
		return numReducers;
	}

	protected final void setNumReducers(int numReducers) {
		this.numReducers = numReducers;
	}

	protected final int getPrecisionGeoHash() {
		return precisionGeoHash;
	}

	protected final void setPrecisionGeoHash(int precisionGeoHash) {
		this.precisionGeoHash = precisionGeoHash;
	}

	protected final String getRutaSalidaFicheros() {
		return rutaSalidaFicheros;
	}

	protected final void setRutaSalidaFicheros(String rutaSalidaFicheros) {
		this.rutaSalidaFicheros = rutaSalidaFicheros;
	}

	protected final String getRutaPaises() {
		return rutaPaises;
	}

	protected final void setRutaPaises(String rutaPaises) {
		this.rutaPaises = rutaPaises;
	}

	protected final String getJobName() {
		return jobName;
	}

	protected final void setJobName(String jobName) {
		this.jobName = jobName;
	}

	protected final String getIndice_archivos() {
		return indice_archivos;
	}

	protected final void setIndice_archivos(String indice_archivos) {
		this.indice_archivos = indice_archivos;
	}

	protected final String getPatronFicheros() {
		return patronFicheros;
	}

	protected final void setPatronFicheros(String patronFicheros) {
		this.patronFicheros = patronFicheros;
	}
}
