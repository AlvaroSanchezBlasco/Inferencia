// Copyright (C) 2015 by Alvaro Sanchez Blasco. All rights reserved.
package mapony.inferencia.jobs.load;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.slf4j.LoggerFactory;

import mapony.inferencia.jobs.InferenciaCustomJob;
import mapony.inferencia.jobs.groupNear.GroupNear;
import mapony.inferencia.mapper.load.LoadMap;
import mapony.inferencia.util.InferenciaMessages;
import mapony.inferencia.util.cte.ElasticSearchClusterCte;
import mapony.inferencia.util.cte.InferenciaCte;
import mapony.inferencia.util.cte.JobNamesCte;
import mapony.inferencia.util.cte.JsonCte;
import mapony.inferencia.util.cte.PropertiesCte;
import mapony.inferencia.util.elasticsearchclient.ElasticSearchClient;
import mapony.inferencia.util.exception.InferenciaException;


public class Load extends InferenciaCustomJob {

	//TODO Mirar el metodo run de esta clase, que no es igual al resto. Hay que modificarlo mejor.
	private String clusterIp;
	private String clusterPort;
	private static String indexClusterES;
	private static String typeClusterES;
	private static String clusterName;

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(final String args[]) throws Exception {
		checkMainClassArgs(args);
		System.exit(getJobExit(args, new Load()));
	}

	@Override
	public int run(String[] args) throws Exception {
		logger = LoggerFactory.getLogger(GroupNear.class);

		loadProperties(args[0]);

		connectoToElasticSearch();

		init();

		// Creamos el job
		Job job = Job.getInstance(getConf(), getJobName());
		job.setJarByClass(Load.class);

		Configuration config = job.getConfiguration();
		setJobConfig(config);

		setOutPuts(config, job);

		if (setJobInputData(config, job) == InferenciaCte.FAIL) {
			return evaluateEndJob(false);
		}

		return evaluateEndJob(job.waitForCompletion(true));
	}

	private void setJobConfig(Configuration config) {
		config.setBoolean("mapred.map.tasks.speculative.execution", false);
		config.set("es.mapping.id", JsonCte.idObject);

		config.set("key.value.separator.in.input.line", " ");
		config.set("es.nodes", getClusterIp() + ":" + getClusterPort());
		config.set("es.resource", indexClusterES + "/" + typeClusterES);
	}

	@Override
	protected void loadProperties(String fileName) throws InferenciaException {
		super.loadProperties(fileName);
		indexClusterES = properties.getProperty(ElasticSearchClusterCte.indexName);
		typeClusterES = properties.getProperty(ElasticSearchClusterCte.typeName);
		clusterName = properties.getProperty(ElasticSearchClusterCte.clusterName);
	}

	private void connectoToElasticSearch() {
		final String[] clusterElasticSearchParams = { indexClusterES, typeClusterES, clusterName };
		new ElasticSearchClient(clusterElasticSearchParams);
		logger.info(InferenciaMessages.connectedToElasticSearchCluster());
	}

	@Override
	protected void init() {
		setJobName(JobNamesCte.loadInElasticSearch);
		setRutaFicheros(properties.getProperty(PropertiesCte.datos_iniciales));
		setClusterIp(properties.getProperty(ElasticSearchClusterCte.ip));
		setClusterPort(properties.getProperty(ElasticSearchClusterCte.port));
	}

	protected void init(Configuration config) {
	}

	@Override
	protected void setJobOutputFormat(Job job) {
		// Output a Elastic Search Output Format
		job.setOutputFormatClass(EsOutputFormat.class);
	}

	@Override
	protected void setMappperOutput(Job job) {
		// Salida del mapper
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);
	}

	protected void setReducerOutput(Job job) {
	}

	@Override
	protected int setJobInputData(Configuration config, Job job) throws InferenciaException {
		try {
			// Recuperamos los ficheros que vamos a procesar, y los anyadimos
			// como datos de entrada
			final FileSystem fs = FileSystem.get(new URI(InferenciaCte.hdfsUri), config);

			// Recuperamos los datos del path origen (data/*.bz2)
			FileStatus[] glob = fs.globStatus(new Path(getRutaFicheros()));

			// Si tenemos datos...
			if (null != glob) {
				if (glob.length > 0) {
					for (FileStatus fileStatus : glob) {
						Path pFich = fileStatus.getPath();
						MultipleInputs.addInputPath(job, pFich, SequenceFileInputFormat.class, LoadMap.class);
					}
				} else {
					return noDataFound();
				}
			}
		} catch (IOException e) {
			throw new InferenciaException(e, e.getMessage());
		} catch (URISyntaxException e) {
			throw new InferenciaException(e, e.getMessage());
		}
		return InferenciaCte.SUCCESS;
	}

	protected void setJobClasses(Job job) {
	}

	private final String getClusterIp() {
		return clusterIp;
	}

	private final void setClusterIp(String clusterIp) {
		this.clusterIp = clusterIp;
	}

	private final String getClusterPort() {
		return clusterPort;
	}

	private final void setClusterPort(String clusterPort) {
		this.clusterPort = clusterPort;
	}

	public Job createJobAndSetJarByClass(Configuration config) throws InferenciaException {
		try {
			final Job job = Job.getInstance(config, getJobName());
			job.setJarByClass(Load.class);
			return job;
		} catch (IOException e) {
			throw new InferenciaException(e, e.getMessage());
		}
	}

	public void setClassLogger() {
		logger = LoggerFactory.getLogger(GroupNear.class);
	}
}
