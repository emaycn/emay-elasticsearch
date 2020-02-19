package cn.emay.elasticsearch.item;

import org.elasticsearch.script.Script;

/**
 * 
 * 更新对象<br/>
 * 脚本数据二选一
 * 
 * @author Frank
 *
 * @param <E>
 */
public class EsUpdateItem<E extends java.io.Serializable> {

	private String id;
	private E data;
	private Script scrpit;

	public EsUpdateItem() {

	}

	/**
	 * 以数据更新
	 * 
	 * @param id
	 * @param data
	 */
	public EsUpdateItem(String id, E data) {
		this.id = id;
		this.setData(data);
	}

	/**
	 * 以脚本更新
	 * 
	 * @param id
	 * @param scrpit
	 */
	public EsUpdateItem(String id, Script scrpit) {
		this.id = id;
		this.scrpit = scrpit;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Script getScrpit() {
		return scrpit;
	}

	public void Script(Script scrpit) {
		this.scrpit = scrpit;
	}

	public E getData() {
		return data;
	}

	public void setData(E data) {
		this.data = data;
	}

}
