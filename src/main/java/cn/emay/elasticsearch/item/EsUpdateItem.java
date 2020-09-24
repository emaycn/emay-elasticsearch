package cn.emay.elasticsearch.item;

import org.elasticsearch.script.Script;

/**
 * 更新对象<br/>
 * 脚本数据二选一
 *
 * @param <E>
 * @author Frank
 */
public class EsUpdateItem<E extends java.io.Serializable> {

    private String id;
    private E data;
    private Script script;

    public EsUpdateItem() {

    }

    /**
     * 以数据更新
     *
     * @param id   id
     * @param data 数据
     */
    public EsUpdateItem(String id, E data) {
        this.id = id;
        this.setData(data);
    }

    /**
     * 以脚本更新
     *
     * @param id     id
     * @param script 脚本
     */
    public EsUpdateItem(String id, Script script) {
        this.id = id;
        this.script = script;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Script getScript() {
        return script;
    }

    public void setScript(Script script) {
        this.script = script;
    }

    public E getData() {
        return data;
    }

    public void setData(E data) {
        this.data = data;
    }

}
