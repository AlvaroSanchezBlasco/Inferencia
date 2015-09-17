package mapony.inferencia.util.elasticsearchclient;

import java.io.IOException;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

public class JsonSettings {

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
										.field("tokenizer", "standard")
										.field("char_filter","html_strip")
										.field("filter", new String[] { "lowercase","unique","max_length","mapony_stop" })
									.endObject()
								.endObject()
								.startObject("filter")
									.startObject("mapony_stop")
										.field("type", "stop")
										.field("stopwords",	new String[] { "_english_","_spanish_" })
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
}
