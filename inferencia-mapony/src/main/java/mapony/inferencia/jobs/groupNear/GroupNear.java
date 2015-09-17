// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.jobs.groupNear;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.LoggerFactory;

import mapony.inferencia.combiner.CommonCombiner;
import mapony.inferencia.jobs.InferenciaCustomJob;
import mapony.inferencia.mapper.groupNear.GroupNearMap;
import mapony.inferencia.reducer.CommonReducer;
import mapony.inferencia.util.cte.InferenciaCte;
import mapony.inferencia.util.cte.JobNamesCte;
import mapony.inferencia.util.cte.PropertiesCte;
import mapony.inferencia.util.exception.InferenciaException;
import mapony.inferencia.writables.RawData;
import mapony.inferencia.writables.arraywritables.ArrayOfRawData;


public class GroupNear extends InferenciaCustomJob {

	@Override
	public Job createJobAndSetJarByClass(Configuration config) throws InferenciaException {
		try {
			final Job job = Job.getInstance(config, getJobName());
			job.setJarByClass(GroupNear.class);
			return job;
		} catch (IOException e) {
			throw new InferenciaException(e, e.getMessage());
		}
	}

	@Override
	public void setClassLogger() {
		logger = LoggerFactory.getLogger(GroupNear.class);
	}
	
	public static void main(String args[]) throws Exception {
		checkMainClassArgs(args);
		System.exit(getJobExit(args, new GroupNear()));
	}

	public void init(Configuration config) {
		setJobName(JobNamesCte.groupNear);

		setIndiceArchivo(properties.getProperty(PropertiesCte.indice_archivo));
		setRutaFicheros(properties.getProperty(PropertiesCte.datos_iniciales) + getIndiceArchivo() + PropertiesCte.ext_archivos);
		setNumReducers(Integer.parseInt(properties.getProperty(PropertiesCte.reducers)));
		setPrecisionGeoHash(Integer.parseInt(properties.getProperty(PropertiesCte.precision)));
		setRutaSalidaFicheros(properties.getProperty(PropertiesCte.salida_datos_job) + getIndiceArchivo());
		setRutaPaises(properties.getProperty(PropertiesCte.paises));

		config.set(PropertiesCte.precision, properties.getProperty(PropertiesCte.precision));
		config.set(PropertiesCte.reservoirSize, properties.getProperty(PropertiesCte.reservoirSize));
	}

	@Override
	protected void setJobOutputFormat(Job job) {
		// La salida de nuestro Job sera del tipo Sequencefile con compresion a nivel de Bloque.
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
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
		job.setOutputValueClass(ArrayOfRawData.class);
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
						MultipleInputs.addInputPath(job, pFich, TextInputFormat.class, GroupNearMap.class);
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
		// Combiner de nuestro Job
		job.setCombinerClass(CommonCombiner.class);

		// Reducer de nuestro Job
		job.setReducerClass(CommonReducer.class);
	}

	@Override
	protected void init() {
	}
}
