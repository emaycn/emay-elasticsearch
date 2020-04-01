package cn.emay.elasticsearch.respoitory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;

import cn.emay.elasticsearch.ann.EsField;
import cn.emay.elasticsearch.ann.EsFieldType;
import cn.emay.elasticsearch.ann.EsIndex;
import cn.emay.elasticsearch.item.EsInsertItem;
import cn.emay.elasticsearch.item.EsUpdateItem;
import cn.emay.json.JsonHelper;
import cn.emay.utils.clazz.ClassUtils;
import cn.emay.utils.result.SuperResult;

/**
 * 基础Dao，作为父类使用，提供常用方法。<br/>
 * es 仅可使用原生sql和jdbctemplate相关用法，不可以使用Hibernate相关用法。
 * 
 * @author Frank
 *
 */
public abstract class PojoEsRepository<E extends java.io.Serializable> extends EsRepository {

	private Class<E> entityClass;

	private EsIndex esIndex;

	@SuppressWarnings("unchecked")
	public PojoEsRepository() {
		this.entityClass = (Class<E>) ((ParameterizedType) (getClass().getGenericSuperclass())).getActualTypeArguments()[0];
		this.esIndex = entityClass.getAnnotation(EsIndex.class);
		if (this.esIndex == null) {
			throw new IllegalArgumentException(entityClass.getName() + " is not es index.");
		}
	}

	public EsIndex getEsIndex() {
		return esIndex;
	}

	/**
	 * 获取索引名字
	 * 
	 * @return
	 */
	public String getIndexName() {
		return getIndexName(null);
	}

	/**
	 * 获取索引名字
	 * 
	 * @param suffix
	 *            后缀
	 * @return
	 */
	public String getIndexName(String suffix) {
		return suffix == null ? esIndex.name() : esIndex.name() + suffix;
	}

	/**
	 * 创建索引
	 */
	public void createIndex() {
		createIndex(null);
	}

	/**
	 * 创建索引,带后缀的
	 */
	public void createIndex(String suffix) {

		String indexName = getIndexName(suffix);

		GetIndexRequest request = new GetIndexRequest(indexName);
		try {
			boolean isHas = this.getClient().indices().exists(request, RequestOptions.DEFAULT);
			if (isHas) {
				return;
			}
		} catch (IOException e1) {
			throw new IllegalArgumentException(e1);
		}

		CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
		Map<String, Object> settings = new HashMap<String, Object>();
		settings.put("number_of_shards", esIndex.shards());
		settings.put("number_of_replicas", esIndex.replicas());
		createIndexRequest.settings(settings);
		try {
			XContentBuilder builder = XContentFactory.jsonBuilder();
			builder.startObject();
			builder.startObject("properties");
			Field[] fields = ClassUtils.getAllFields(entityClass);
			for (Field field : fields) {
				EsField ef = field.getAnnotation(EsField.class);
				if (ef == null) {
					continue;
				}
				builder.startObject(field.getName());
				builder.field("type", ef.type().getType());
				if (ef.type().equals(EsFieldType.TEXT)) {
					builder.startObject("fields");
					builder.startObject("key");
					builder.field("type", EsFieldType.KEYWORD.getType());
					builder.endObject();
					builder.endObject();
					builder.field("analyzer", "standard");
				}
				if (ef.type().getFormat() != null) {
					builder.field("format", ef.type().getFormat());
				}
				builder.endObject();
			}
			builder.endObject();
			builder.endObject();
			createIndexRequest.mapping(builder);
			this.getClient().indices().create(createIndexRequest, RequestOptions.DEFAULT);
		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * 删除索引
	 */
	public void deleteIndex() {
		deleteIndex(null);
	}

	/**
	 * 删除索引,带后缀的
	 */
	public void deleteIndex(String suffix) {
		String indexName = getIndexName(suffix);

		GetIndexRequest request = new GetIndexRequest(indexName);
		try {
			boolean isHas = this.getClient().indices().exists(request, RequestOptions.DEFAULT);
			if (!isHas) {
				return;
			}
		} catch (IOException e1) {
			throw new IllegalArgumentException(e1);
		}

		try {
			DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(indexName);
			this.getClient().indices().delete(deleteIndexRequest, RequestOptions.DEFAULT);
		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * 新增数据
	 */
	public void save(String id, E data) {
		save(null, id, data);
	}

	/**
	 * 新增数据
	 */
	public void save(String suffix, String id, E data) {
		String indexName = getIndexName(suffix);
		IndexRequest indexRequest = new IndexRequest(indexName);
		indexRequest.id(id);
		indexRequest.source(JsonHelper.toJsonStringWithoutNullAndSSS(data), XContentType.JSON);
		try {
			this.getClient().index(indexRequest, RequestOptions.DEFAULT);
		} catch (IOException e1) {
			throw new IllegalArgumentException(e1);
		}
	}

	/**
	 * 新增数据
	 */
	public SuperResult<String[]> save(List<EsInsertItem<E>> datas) {
		return save(null, datas);
	}

	/**
	 * 新增数据，带后缀
	 */
	public SuperResult<String[]> save(String suffix, List<EsInsertItem<E>> datas) {
		String indexName = getIndexName(suffix);
		BulkRequest bulkRequest = new BulkRequest();
		datas.forEach(data -> {
			IndexRequest indexRequest = new IndexRequest(indexName);
			indexRequest.id(data.getId());
			indexRequest.source(JsonHelper.toJsonStringWithoutNullAndSSS(data.getData()), XContentType.JSON);
			bulkRequest.add(indexRequest);
		});
		return bathOper(bulkRequest);
	}

	/**
	 * 更新,脚本更新
	 */
	public void update(String id, Script scrpit) {
		update(null, id, null, scrpit);
	}

	/**
	 * 更新,字段更新
	 */
	public void update(String id, E data) {
		update(null, id, data, null);
	}

	/**
	 * 更新，带后缀,脚本更新
	 */
	public void update(String suffix, String id, Script scrpit) {
		update(suffix, id, null, scrpit);
	}

	/**
	 * 更新，带后缀,字段更新
	 */
	public void update(String suffix, String id, E data) {
		update(suffix, id, data, null);
	}

	/**
	 * 更新，带后缀,支持脚本更新和字段更新
	 */
	private void update(String suffix, String id, E data, Script scrpit) {
		String indexName = getIndexName(suffix);
		UpdateRequest updateRequest = new UpdateRequest(indexName, id);
		if (scrpit != null) {
			updateRequest.script(scrpit);
		} else if (data != null) {
			updateRequest.doc(JsonHelper.toJsonStringWithoutNullAndSSS(data), XContentType.JSON);
		} else {
			throw new IllegalArgumentException("data or scrpit must not null");
		}
		try {
			this.getClient().update(updateRequest, RequestOptions.DEFAULT);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * 更新，带后缀,支持脚本更新和字段更新
	 */
	public SuperResult<String[]> update(List<EsUpdateItem<E>> datas) {
		return update(null, datas);
	}

	/**
	 * 更新，带后缀,支持脚本更新和字段更新
	 */
	public SuperResult<String[]> update(String suffix, List<EsUpdateItem<E>> datas) {
		String indexName = getIndexName(suffix);
		BulkRequest bulkRequest = new BulkRequest();
		datas.forEach(data -> {
			UpdateRequest updateRequest = new UpdateRequest(indexName, data.getId());
			if (data.getScrpit() != null) {
				updateRequest.script(data.getScrpit());
			} else if (data.getData() != null) {
				updateRequest.doc(JsonHelper.toJsonStringWithoutNullAndSSS(data.getData()), XContentType.JSON);
			} else {
				throw new IllegalArgumentException("data or scrpit must not null");
			}
			bulkRequest.add(updateRequest);
		});
		return bathOper(bulkRequest);
	}

	/**
	 * 删除数据
	 */
	public void delete(String id) {
		delete(null, id);
	}

	/**
	 * 删除数据，带后缀
	 */
	public void delete(String suffix, String id) {
		String indexName = getIndexName(suffix);
		DeleteRequest deleteRequest = new DeleteRequest(indexName, id);
		try {
			this.getClient().delete(deleteRequest, RequestOptions.DEFAULT);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}

	/**
	 * 删除数据
	 */
	public SuperResult<String[]> delete(List<String> ids) {
		return delete(null, ids);
	}

	/**
	 * 删除数据，带后缀
	 */
	public SuperResult<String[]> delete(String suffix, List<String> ids) {
		String indexName = getIndexName(suffix);
		BulkRequest bulkRequest = new BulkRequest();
		ids.forEach(id -> {
			DeleteRequest deleteRequest = new DeleteRequest(indexName, id);
			bulkRequest.add(deleteRequest);
		});
		return bathOper(bulkRequest);
	}

	/**
	 * 批量操作，返回操作失败的ID
	 */
	public SuperResult<String[]> bathOper(BulkRequest bulkRequest) {
		List<String> errorIds = new ArrayList<>();
		try {
			BulkResponse bulkResponse = this.getClient().bulk(bulkRequest, RequestOptions.DEFAULT);
			BulkItemResponse[] items = bulkResponse.getItems();
			Arrays.stream(items).forEach(item -> {
				if (item.isFailed()) {
					errorIds.add(item.getFailure().getId());
				}
			});
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
		if (errorIds.size() == 0) {
			return SuperResult.rightResult();
		} else {
			return SuperResult.badResult("", errorIds.toArray(new String[errorIds.size()]));
		}
	}
	
	/**
	 * 根据ID查询
	 * 
	 * @param id
	 * @return
	 */
	public E findById(String id) {
		return findById(null, id);
	}

	/**
	 * 根据ID查询
	 * 
	 * @param id
	 * @return
	 */
	public E findById(String suffix, String id) {
		GetRequest get = new GetRequest(this.getIndexName(suffix), id);
		try {
			GetResponse getRes = this.getClient().get(get, RequestOptions.DEFAULT);
			String source = getRes.getSourceAsString();
			return JsonHelper.fromJson(entityClass, source);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		}
	}
}
