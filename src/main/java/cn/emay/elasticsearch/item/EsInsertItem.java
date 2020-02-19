package cn.emay.elasticsearch.item;

/**
 * ES 插入基本对象
 * 
 * @author frank
 *
 * @param <E>
 */
public class EsInsertItem<E extends java.io.Serializable> {

	private String id;

	private E data;

	public EsInsertItem() {

	}

	public EsInsertItem(Long id, E data) {
		super();
		this.id = String.valueOf(id);
		this.data = data;
	}

	public EsInsertItem(String id, E data) {
		super();
		this.id = id;
		this.data = data;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public E getData() {
		return data;
	}

	public void setData(E data) {
		this.data = data;
	}

}
