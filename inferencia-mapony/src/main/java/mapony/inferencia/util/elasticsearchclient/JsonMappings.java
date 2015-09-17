package mapony.inferencia.util.elasticsearchclient;

import java.io.IOException;

import mapony.inferencia.util.cte.JsonCte;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;

public class JsonMappings {

	/**
	 * Mapeo de datos en el Json
	 * 
	 * @return estructura del json que vamos a generar en nuestro cluster
	 */
	public static XContentBuilder buildJsonMappings() {
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
							.startObject(JsonCte.idObject)
								.field("type", "string")
								.field("store", true)
								.field("index", "not_analyzed")
								.field("include_in_all",false)
							.endObject()
							.startObject(JsonCte.tituloObject)
								.field("type", "string")
								.field("store", true)
								.field("index", "analyzed")
							.endObject()
							.startObject(JsonCte.descripcionObject)
								.field("type", "string")
								.field("store", true)
								.field("index", "analyzed")
//								.field("ignore_above", 200)
							.endObject()
							.startObject(JsonCte.userTagsObject)
								.field("type", "string")
								.field("store", true)
								.field("index", "analyzed")
//								.field("ignore_above", 200)
							.endObject()
							.startObject(JsonCte.machineTagsObject)
								.field("type", "string")
								.field("store", true)
								.field("index", "analyzed")
//								.field("ignore_above", 200)
							.endObject()
							.startObject(JsonCte.locationObject)
								.field("type", "geo_point")
								.field("geohash", true)
								.field("geohash_prefix", true)
								.field("include_in_all", false)
								.field("geohash_precision", 10)
								.field("lat_lon", "true")
							.endObject()
							.startObject(JsonCte.fotoObject)
								.field("type", "string")
								.field("store", true)
								.field("index", "not_analyzed")
								.field("include_in_all", false)
							.endObject()
							.startObject(JsonCte.captureDeviceObject)
								.field("type", "string")
								.field("store", true)
								.field("index", "analyzed")
//								.field("ignore_above", 200)
							.endObject()
							.startObject(JsonCte.fechaCapturaObject)
								.field("type", "date")
								.field("format", "yyyy-MM-dd")
								.field("store", true)
								.field("index", "analyzed")
//								.field("ignore_above", 200)
							.endObject()
//							.startObject(MaponyJsonCte.ciudadObject)
//								.field("type", "string")
//								.field("store", true)
//								.field("index", "analyzed")
//								.field("ignore_above", 200)
//							.endObject()
						.endObject()
					.endObject();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return builder;
	}
	
}
