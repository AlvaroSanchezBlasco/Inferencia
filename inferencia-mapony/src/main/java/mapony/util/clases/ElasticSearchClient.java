package mapony.util.clases;

import static org.elasticsearch.node.NodeBuilder.nodeBuilder;

import java.io.IOException;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mapony.util.constantes.MaponyJsonCte;

/**
 * @author Alvaro Sanchez Blasco
 *
 *	<p>Cliente para la conexion a Elastic Search
 *
 */
public class ElasticSearchClient {

	private String index;
	private String type;
	private Client client;
	private Node node;
	private final Logger logger = LoggerFactory.getLogger(ElasticSearchClient.class);

	/**
	 * Crea la conexión con ES, en un cluster determinado, creando un indice que llega como parametro.
	 * 
	 * @param index
	 *            - Indice.
	 * @param type
	 *            - tipo.
	 * @param clusterName
	 *            - Nombre del cluster de ES.
	 */
	public ElasticSearchClient(String index, String type, String clusterName) {
		setIndex(index);
		setType(type);
		setNode(nodeBuilder().clusterName(clusterName).client(true).node());
		setClient(getNode().client());
		if (existeIndiceEnElCluster()) {
			borrarIndice();
			crearIndice();
			getLogger().info("Indice: " + getIndex() + "/" + getType() + " borrado y creado");
		} else {
			crearIndice();
			getLogger().info("Indice: " + getIndex() + "/" + getType() + " nuevo y creado");
		}
	}

	/**
	 * Comprueba si el indice que queremos dar de alta, ya existe en el cluster.
	 * 
	 * @param index
	 *            - Indice a comprobar.
	 * @return true si ya existe.
	 */
	private boolean existeIndiceEnElCluster() {
		ActionFuture<IndicesExistsResponse> existe = getClient().admin().indices().exists(new IndicesExistsRequest(getIndex()));
		IndicesExistsResponse actionGet = existe.actionGet();
		return actionGet.isExists();
	}

	
	/**
	 * Crea el indice que llega como parametro en el cluster de ES.
	 * 
	 * @param index
	 *            - Indice a crear.
	 */
	private void crearIndice() {
		XContentBuilder typemapping = buildJsonMappings();
		XContentBuilder settings = buildJsonSettings();

		getClient().admin().indices().create(new CreateIndexRequest(getIndex()).settings(settings).mapping(getType(), typemapping)).actionGet();
	}

	/**
	 * Método que borra, si existe, el indice en el cluster de ES.
	 * 
	 * @param client
	 * @param index
	 */
	private void borrarIndice() {
		try {
			DeleteIndexResponse delete = getClient().admin().indices().delete(new DeleteIndexRequest(getIndex())).actionGet();
			if (!delete.isAcknowledged()) {
			} else {
			}
		} catch (Exception e) {
		}
	}

	/**
	 * @return XContentBuilder con la configuracion del contenido del cluster
	 */
	public static XContentBuilder buildJsonSettings() {
		XContentBuilder builder = null;
		try {
			builder = XContentFactory.jsonBuilder();
			builder.startObject()
						.startObject("index")
							.startObject("analysis")
								.startObject("analyzer")
									.startObject("mapony_analyzer")
										.field("type", "custom")
										.field("tokenizer", "mapony_tokenizer_standard")
										.field("char_filter","html_strip")
										.field("filter", new String[] {"lowercase","unique","max_length","mapony_stop", "mapony_delimiter"})
									.endObject()
								.endObject()
								.startObject("tokenizer")
									.startObject("mapony_tokenizer_standard")
										.field("type", "standard")
										.field("max_token_length","500")
									.endObject()
								.endObject()
								.startObject("filter")
									.startObject("mapony_stop")
										.field("type", "stop")
										.field("generate_word_parts",false)
										.field("generate_number_parts",false)
										.field("split_on_numerics",false)
									.endObject()
								.endObject()
								.startObject("filter")
									.startObject("mapony_delimiter")
										.field("type", "word_delimiter")
										.field("stopwords",
												new String[] {
													"_english_,ante,bajo,cabe,con,contra,de,desde,durante,en,entre,"
													+ "hacia,hasta,mediante,para,por,según,segun,sin,so,sobre,tras,"
													+ "versus,vía,el,la,lo,los,las,les" })
									.endObject()
								.endObject()
								.startObject("filter")
									.startObject("max_length")
										.field("type", "length")
										.field("max",10)
										.field("min",4)
									.endObject()
								.endObject()
							.endObject()
						.endObject()
					.endObject();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return builder;
	}
	
	/**
	 * Mapeo de datos en el Json
	 * 
	 * @return estructura del json que vamos a generar en nuestro cluster
	 */
	public static  XContentBuilder buildJsonMappings() {
		XContentBuilder builder = null;
		try {

			builder = XContentFactory.jsonBuilder();
			builder.startObject()
						.startObject("_id")
							.field("path", "id")
						.endObject()
						.startObject("_all")
							.field("enabled", true)
							.field("analyzer", "mapony_analyzer")
						.endObject()
						.startObject("properties")
							.startObject(MaponyJsonCte.idObject)
								.field("type", "string")
								.field("store", true)
								.field("index", "not_analyzed")
								.field("include_in_all",false)
							.endObject()
							.startObject(MaponyJsonCte.tituloObject)
								.field("type", "string")
								.field("store", true)
								.field("index", "analyzed")
							.endObject()
							.startObject(MaponyJsonCte.descripcionObject)
								.field("type", "string")
								.field("store", true)
								.field("index", "analyzed")
//								.field("ignore_above", 200)
							.endObject()
							.startObject(MaponyJsonCte.userTagsObject)
								.field("type", "string")
								.field("store", true)
								.field("index", "analyzed")
//								.field("ignore_above", 200)
							.endObject()
							.startObject(MaponyJsonCte.machineTagsObject)
								.field("type", "string")
								.field("store", true)
								.field("index", "analyzed")
//								.field("ignore_above", 200)
							.endObject()
							.startObject(MaponyJsonCte.locationObject)
								.field("type", "geo_point")
								.field("geohash", true)
								.field("geohash_prefix", true)
								.field("include_in_all", false)
								.field("geohash_precision", 10)
								.field("lat_lon", "true")
							.endObject()
							.startObject(MaponyJsonCte.fotoObject)
								.field("type", "string")
								.field("store", true)
								.field("index", "not_analyzed")
								.field("include_in_all", false)
							.endObject()
							.startObject(MaponyJsonCte.captureDeviceObject)
								.field("type", "string")
								.field("store", true)
								.field("index", "analyzed")
//								.field("ignore_above", 200)
							.endObject()
							.startObject(MaponyJsonCte.fechaCapturaObject)
								.field("type", "date")
								.field("format", "yyyy-MM-dd")
								.field("store", true)
								.field("index", "analyzed")
//								.field("ignore_above", 200)
							.endObject()
							.startObject(MaponyJsonCte.ciudadObject)
								.field("type", "string")
								.field("store", true)
								.field("index", "analyzed")
//								.field("ignore_above", 200)
							.endObject()
						.endObject()
					.endObject();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return builder;
	}
	
	/**
	 * @return the index
	 */
	private final String getIndex() {
		return index;
	}

	/**
	 * @param index the index to set
	 */
	private final void setIndex(String index) {
		this.index = index;
	}

	/**
	 * @return the type
	 */
	private final String getType() {
		return type;
	}

	/**
	 * @param type the type to set
	 */
	private final void setType(String type) {
		this.type = type;
	}

	/**
	 * @return the client
	 */
	private final Client getClient() {
		return client;
	}

	/**
	 * @param client the client to set
	 */
	private final void setClient(Client client) {
		this.client = client;
	}

	/**
	 * @return the node
	 */
	private final Node getNode() {
		return node;
	}

	/**
	 * @param node the node to set
	 */
	private final void setNode(Node node) {
		this.node = node;
	}

	/**
	 * @return the logger
	 */
	private final Logger getLogger() {
		return logger;
	}
	
}