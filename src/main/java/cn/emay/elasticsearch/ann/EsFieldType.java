package cn.emay.elasticsearch.ann;

/**
 * ES 字段类型<br/>
 * mapping映射
 * 
 * @author frank
 *
 */
public enum EsFieldType {
	//
	DATE("date", "yyyy-MM-dd HH:mm:ss SSS||yyyy-MM-dd||epoch_millis"),
	//
	INTEGER("integer", null),
	//
	KEYWORD("keyword", null),
	//
	LONG("long", null),
	//
	TEXT("text", null),
	//
	DOUBLE("double", null),
	//
	BOOLEAN("boolean", null),;

	private String type;

	private String format;

	private EsFieldType(String type, String format) {
		this.setType(type);
		this.format = format;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

}
