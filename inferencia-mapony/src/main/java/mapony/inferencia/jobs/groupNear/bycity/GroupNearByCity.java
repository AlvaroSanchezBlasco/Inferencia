package mapony.inferencia.jobs.groupNear.bycity;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.LoggerFactory;

import mapony.inferencia.combiner.CommonCombiner;
import mapony.inferencia.jobs.groupNear.GroupNear;
import mapony.inferencia.mapper.groupNear.GroupNearByCityMap;
import mapony.inferencia.partitioner.CityPartitioner;
import mapony.inferencia.reducer.MultipleOutputsReducer;
import mapony.inferencia.util.cte.InferenciaCte;
import mapony.inferencia.util.cte.JobNamesCte;
import mapony.inferencia.util.cte.PropertiesCte;
import mapony.inferencia.util.exception.InferenciaException;


public class GroupNearByCity extends GroupNear {


	public Job createJobAndSetJarByClass(Configuration config) throws InferenciaException {
		try {
			final Job job = Job.getInstance(config, getJobName());
			job.setJarByClass(GroupNearByCity.class);
			return job;
		} catch (IOException e) {
			throw new InferenciaException(e, e.getMessage());
		}
	}
	
	public void setClassLogger(){
		logger = LoggerFactory.getLogger(GroupNearByCity.class);
	}
	
	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String args[]) throws Exception {
		checkMainClassArgs(args);
		System.exit(getJobExit(args, new GroupNearByCity()));
	}

	@Override
	public void init(Configuration config) {
		setJobName(JobNamesCte.groupNearByCity);
		
		setIndiceArchivo(properties.getProperty(PropertiesCte.indice_archivo));
		setRutaFicheros(properties.getProperty(PropertiesCte.datos_iniciales) + getIndiceArchivo() + PropertiesCte.ext_archivos);
		setNumReducers(Integer.parseInt(properties.getProperty(PropertiesCte.reducers)));
		setRutaSalidaFicheros(properties.getProperty(PropertiesCte.salida_datos_job) + getIndiceArchivo());

		config.set(PropertiesCte.precision, properties.getProperty(PropertiesCte.precision));
		config.set(PropertiesCte.reservoirSize, properties.getProperty(PropertiesCte.reservoirSize));
	}

	@Override
	protected int setJobInputData(Configuration config, Job job) throws InferenciaException {

		try {
			// Recuperamos los ficheros que vamos a procesar, y los anyadimos como datos de entrada
			final FileSystem fs = FileSystem.get(new URI(InferenciaCte.hdfsUri), config);

			// Recuperamos los datos a procesar del path origen (data/*.bz2)
			FileStatus[] glob = fs.globStatus(new Path(getRutaFicheros()));

			// Si tenemos datos...
			if (null != glob) {
				if (glob.length > 0) {
					for (FileStatus fileStatus : glob) {
						Path pFich = fileStatus.getPath();
						// MultipleInputs
						MultipleInputs.addInputPath(job, pFich, TextInputFormat.class,
								GroupNearByCityMap.class);
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

	@Override
	protected void setJobClasses(Job job) {
		job.setCombinerClass(CommonCombiner.class);
		job.setReducerClass(MultipleOutputsReducer.class);
		job.setPartitionerClass(CityPartitioner.class);
	}
}
