package mapony.inferencia.util.elasticsearchclient;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;
import mapony.inferencia.util.InferenciaMessages;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alvaro Sanchez Blasco
 *         <p>
 *         Cliente para la conexion a Elastic Search
 *         <p>
 *         Elastic Search Client
 */
public class ElasticSearchClient {

	private String index;
	private String type;
	private Client client;
	private Node node;
	private final Logger logger = LoggerFactory.getLogger(ElasticSearchClient.class);

	/**
	 * Crea la conexion con ES, en un cluster determinado, creando un indice que llega como parametro.
	 * 
	 * @param clusterElasticSearchAttributes
	 *            <ul>
	 *            <li>Index.
	 *            <li>Type.
	 *            <li>clusterName.
	 *            </ul>
	 */
	public ElasticSearchClient(final String[] clusterElasticSearchAttributes) {
		// Example setup for a Client Object
		Client esClient = NodeBuilder.nodeBuilder().client(true).node().client();
		
		setAttributes(clusterElasticSearchAttributes);
		createIndex();
	}

	private void setAttributes(final String[] clusterElasticSearchAttributes){
		setIndex(clusterElasticSearchAttributes[0]);
		setType(clusterElasticSearchAttributes[1]);
		setNode(nodeBuilder().clusterName(clusterElasticSearchAttributes[2]).client(true).node());
		setClient(getNode().client());
	}
	
	private void createIndex(){
		if (indexExists()) {
			deleteIndexOnCluster();
			createIndexOnCluster();
			getLogger().info(InferenciaMessages.indexDeletedAndCreated(getIndex(), getType()));
		} else {
			createIndexOnCluster();
			getLogger().info(InferenciaMessages.newIndexCreated(getIndex(), getType()));
		}		
	}
	
	/**
	 * Comprueba si el indice que queremos dar de alta, ya existe en el cluster.
	 * 
	 * @param index
	 *            - Indice a comprobar.
	 * @return true si ya existe.
	 */
	private boolean indexExists() {
		ActionFuture<IndicesExistsResponse> existe = getClient().admin().indices()
				.exists(new IndicesExistsRequest(getIndex()));
		IndicesExistsResponse actionGet = existe.actionGet();
		return actionGet.isExists();
	}

	/**
	 * Crea el indice que llega como parametro en el cluster de ES.
	 * 
	 * @param index
	 *            - Indice a crear.
	 */
	private void createIndexOnCluster() {
		XContentBuilder typemapping = JsonMappings.buildJsonMappings();
		XContentBuilder settings = JsonSettings.buildJsonSettings();
		
		// Ejemplo de creacion de un indice
		getClient().admin().indices().prepareCreate(getIndex()).execute().actionGet();
		
//		getClient().admin().indices()
//				.create(new CreateIndexRequest(getIndex()).settings(settings).mapping(getType(), typemapping))
//				.actionGet();
	}

	/**
	 * Metodo que borra, si existe, el indice en el cluster de ES.
	 * 
	 * @param client
	 * @param index
	 */
	private void deleteIndexOnCluster() {
		try {
			DeleteIndexResponse delete = getClient().admin().indices().delete(new DeleteIndexRequest(getIndex()))
					.actionGet();
			if (!delete.isAcknowledged()) {
			} else {
			}
		} catch (Exception e) {
		}
	}

	private final String getIndex() {
		return index;
	}

	private final void setIndex(String index) {
		this.index = index;
	}

	private final String getType() {
		return type;
	}

	private final void setType(String type) {
		this.type = type;
	}

	private final Client getClient() {
		return client;
	}

	private final void setClient(Client client) {
		this.client = client;
	}

	private final Node getNode() {
		return node;
	}

	private final void setNode(Node node) {
		this.node = node;
	}

	private final Logger getLogger() {
		return logger;
	}
}