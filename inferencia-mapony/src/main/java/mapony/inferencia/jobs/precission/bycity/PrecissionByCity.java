// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.jobs.precission.bycity;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import mapony.inferencia.combiner.CommonCombiner;
import mapony.inferencia.jobs.precission.Precission;
import mapony.inferencia.partitioner.CityPartitioner;
import mapony.inferencia.reducer.MultipleOutputsReducer;
import mapony.inferencia.util.cte.JobNamesCte;
import mapony.inferencia.util.cte.PropertiesCte;
import mapony.inferencia.util.exception.InferenciaException;


public class PrecissionByCity extends Precission {

	public static void main(final String args[]) throws Exception {
		checkMainClassArgs(args);
		System.exit(ToolRunner.run(new PrecissionByCity(), args));
	}
	
	@Override
	public Job createJobAndSetJarByClass(Configuration config) throws InferenciaException {
		try {
			final Job job = Job.getInstance(config, getJobName());
			job.setJarByClass(PrecissionByCity.class);
			return job;
		} catch (IOException e) {
			throw new InferenciaException(e, e.getMessage());
		}
	}

	@Override
	protected void init(Configuration config) {
		setJobName(JobNamesCte.precissionByCity);

		setIndice_archivos(properties.getProperty(PropertiesCte.indice_archivo));
		setPatronFicheros(properties.getProperty(PropertiesCte.patron_ficheros));

		// Completamos la ruta con la mascara de ficheros a buscar
		setRutaFicheros(properties.getProperty(PropertiesCte.datos_iniciales) + getIndice_archivos() + getPatronFicheros());

		// Anyadimos el indice a la salida del job para diferenciar los datos de cada job con el archivo original
		setRutaSalidaFicheros(properties.getProperty(PropertiesCte.salida_datos_job) + getIndice_archivos());
		setNumReducers(Integer.parseInt(properties.getProperty(PropertiesCte.reducers)));

		config.set(PropertiesCte.precision, properties.getProperty(PropertiesCte.precision));
		config.set(PropertiesCte.reservoirSize, properties.getProperty(PropertiesCte.reservoirSize));
	}

	protected void init() {
	}

	@Override
	protected void setJobOutputFormat(Job job) {
		super.setJobOutputFormat(job);
	}

	@Override
	protected void setMappperOutput(Job job) {
		super.setMappperOutput(job);
	}

	@Override
	protected void setReducerOutput(Job job) {
		super.setReducerOutput(job);
	}

	@Override
	protected void setJobClasses(Job job) {
		// Combiner de nuestro Job
		job.setCombinerClass(CommonCombiner.class);

		// Reducer de nuestro Job
		job.setReducerClass(MultipleOutputsReducer.class);
		
		job.setPartitionerClass(CityPartitioner.class);
	}
}