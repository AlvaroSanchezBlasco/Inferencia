package mapony.inferencia.util;

import mapony.inferencia.util.cte.InferenciaCte;

public class InferenciaMessages {
	
	private static final String JOB_BEGINS = "\nComienza la ejecucion del job %s";
	private static final String JOB_END = "\nLa ejecucion del Job %s ha finalizado %s\n";
	
	private static final String JOB_ENDED_SUCCESSFUL = "correctamente";
	private static final String JOB_ENDED_NO_COMPLETION = "con error";

	private static final String CONNECTED_TO_THE_CLUSTER_OF_ELASTIC_SEARCH = "Conexion realizada al cluster de ElasticSearch";

	private static final String ELASTIC_SEARCH_NEW_INDEX_CREATED = "Index %s"+InferenciaCte.PIPE+"%s is new and has been created";
	private static final String ELASTIC_SEARCH_INDEX_DELETED_AND_CREATED = "Index %s"+InferenciaCte.PIPE+"%s existed, has been deleted and created";
	
	private static final String NO_DATA_IN_SPECIFIED_PATH = "No existen datos en la ruta especificada\nNo data in the specified path: %s";

	/**
	 * Mensaje que informa que el job que recibe como parametro ha comenzado a ejecutarse 
	 * 
	 * @param jobName
	 * @return Mensaje de inicio del job
	 */
	public static final String jobBegins(final String jobName) {
		return String.format(JOB_BEGINS, jobName);
	}
	
	/**
	 * Mensaje que informa de la finalizacion del job que recibe como parametro y como ha sido la finalizacion del mismo
	 * @param jobName - Nombre del Job
	 * @param howHasTheJobEnded - Como ha finalizado el job (correctamente o con error)
	 * @return Mensaje de finalizacion del job 
	 */
	private static final String jobEnds(final String jobName, final String howHasTheJobEnded) {
		return String.format(JOB_END, jobName, howHasTheJobEnded);
	}

	/**
	 * Mensaje que informa de la finalizacion del job que recibe como parametro de forma satisfactoria
	 * 
	 * @param jobName
	 * @return Mensaje de fin del job
	 */
	public static final String jobEndedSuccessful(final String jobName) {
		return jobEnds(jobName, JOB_ENDED_SUCCESSFUL);
	}

	/**
	 * Mensaje que informa de la finalizacion con error del job que recibe como parametro
	 * 
	 * @param jobName
	 * @return Mensaje de fin del job
	 */
	public static final String jobEndedNoCompletion(final String jobName) {
		return jobEnds(jobName, JOB_ENDED_NO_COMPLETION);
	}

	public static final String connectedToElasticSearchCluster() {
		return CONNECTED_TO_THE_CLUSTER_OF_ELASTIC_SEARCH;
	}
	
	public static final String newIndexCreated(final String indexName, final String typeName){
		return String.format(ELASTIC_SEARCH_NEW_INDEX_CREATED, indexName, typeName);
	}

	public static final String indexDeletedAndCreated(final String indexName, final String typeName){
		return String.format(ELASTIC_SEARCH_INDEX_DELETED_AND_CREATED, indexName, typeName);
	}
	
	public static final String noDataInSpecifiedPath(final String path) {
		return String.format(NO_DATA_IN_SPECIFIED_PATH, path);
	}
}
