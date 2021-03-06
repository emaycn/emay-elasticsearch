package cn.emay.elasticsearch.respoitory;

import cn.emay.json.GsonHelper;
import cn.emay.json.JsonHelper;
import cn.emay.utils.db.common.Page;
import com.google.gson.reflect.TypeToken;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;

/**
 * 基础Dao，作为父类使用，提供常用方法。<br/>
 * es 仅可使用原生sql和jdbctemplate相关用法，不可以使用Hibernate相关用法。</br>
 * 注意：sql查询出来的时间是UTC时间，需要减掉8个小时
 *
 * @author Frank
 */
public abstract class EsRepository {

    /**
     * es客户端
     */
    protected abstract RestHighLevelClient getClient();

    /**
     * es JdbcTemplate客户端
     */
    protected abstract JdbcTemplate getJdbcTemplate();

    /**
     * 8小时时差
     */
    private final long TIME_8_H = 8L * 60L * 60L * 1000L;

    /**
     * 根据ID分页查询
     */
    public <T> Page<T> queryPage(String sql, boolean isNextPage, Long startId, int start, int limit, TypeToken<List<T>> type) {
        String startValue = startId == null ? null : startId.toString();
        return queryPage(sql, isNextPage, "id", startValue, start, limit, type);
    }

    /**
     * 根据ID分页查询<br/>
     */
    public <T> List<T> queryListForPage(String sql, boolean isNextPage, Long startId, int limit, TypeToken<List<T>> type) {
        String startValue = startId == null ? null : startId.toString();
        return queryListForPage(sql, isNextPage, "id", startValue, limit, type);
    }

    /**
     * 根据时间分页查询
     */
    public <T> Page<T> queryTimePage(String sql, boolean isNextPage, String startTime, int start, int limit, TypeToken<List<T>> type) {
        return queryPage(sql, isNextPage, "createTime", startTime, start, limit, type);
    }

    /**
     * 根据时间分页查询<br/>
     * 必须写order
     */
    public <T> List<T> queryListForTimePage(String sql, boolean isNextPage, String startTime, int limit, TypeToken<List<T>> type) {
        return queryListForPage(sql, isNextPage, "createTime", startTime, limit, type);
    }

    /**
     * 分页查询
     */
    public <T> Page<T> queryPage(String sql, boolean isNextPage, String startFieldName, String startValue, int start, int limit, TypeToken<List<T>> type) {
        int count = queryCount(sql);
        List<T> list = queryListForPage(sql, isNextPage, startFieldName, startValue, limit, type);
        Page<T> page = new Page<>();
        page.setNumByStartAndLimit(start, limit, count);
        page.setList(list);
        return page;
    }

    /**
     * 分页查询<br/>
     * 必须写order
     */
    public <T> List<T> queryListForPage(String sql, boolean isNextPage, String startFieldName, String startValue, int limit, TypeToken<List<T>> type) {
        int orderbyindex = sql.toLowerCase().lastIndexOf(" order by");
        if (orderbyindex <= 0) {
            throw new IllegalArgumentException(" ES sql 分页查询必须写 order 进行排序 ");
        }
        if (startFieldName == null) {
            throw new IllegalArgumentException(" startFieldName 不能为空 ");
        }
        boolean isDesc = sql.trim().toLowerCase().endsWith("desc");
        boolean isHasAsc = sql.trim().toLowerCase().endsWith("asc");
        String mainSql = sql.substring(0, orderbyindex);
        String ordersql = sql.substring(orderbyindex);
        StringBuilder querySql = new StringBuilder(mainSql);
        if (startValue != null) {
            if (sql.contains(" where ")) {
                querySql.append(" and ");
            } else {
                querySql.append(" where ");
            }
            if (isDesc) {
                if (isNextPage) {
                    querySql.append(" ").append(startFieldName).append(" < '").append(startValue).append("'");
                } else {
                    querySql.append(" ").append(startFieldName).append(" > '").append(startValue).append("'");
                    // 反转order by ，否则查到了第一页的数据
                    String[] items = ordersql.split(" ");
                    for (int i = items.length - 1; i >= 0; i--) {
                        if (items[i].equalsIgnoreCase("desc")) {
                            items[i] = "asc";
                            break;
                        }
                    }
                    StringBuilder ret = new StringBuilder();
                    for (String item : items) {
                        ret.append(item).append(" ");
                    }
                    ordersql = ret.toString();
                }
            } else {
                if (isNextPage) {
                    querySql.append(" ").append(startFieldName).append(" > '").append(startValue).append("'");
                } else {
                    querySql.append(" ").append(startFieldName).append(" < '").append(startValue).append("'");
                    // 反转order by ，否则查到了第一页
                    if (isHasAsc) {
                        String[] items = ordersql.split(" ");
                        for (int i = items.length - 1; i >= 0; i--) {
                            if (items[i].equalsIgnoreCase("asc")) {
                                items[i] = "desc";
                                break;
                            }
                        }
                        StringBuilder ret = new StringBuilder();
                        for (String item : items) {
                            ret.append(item).append(" ");
                        }
                        ordersql = ret.toString();
                    } else {
                        ordersql += " desc ";
                    }
                }
            }
        }
        querySql.append(ordersql);
        querySql.append(" limit ").append(limit);
        List<T> list = queryList(querySql.toString(), type);
        if (list != null && list.size() > 0) {
            if (!isNextPage) {
                Collections.reverse(list);
            }
        }
        return list;
    }

    /**
     * 查询
     */
    public <T> List<T> queryList(String sql, TypeToken<List<T>> type) {
        if (type == null) {
            throw new NullPointerException("type is null");
        }
        if (sql == null) {
            throw new NullPointerException("sql is null");
        }
        List<Map<String, Object>> result = this.getJdbcTemplate().queryForList(sql);
        List<T> e = new ArrayList<>();
        if (result.size() > 0) {
            // 日期问题，暂时手工处理
            result.forEach(item -> {
                Map<String, Object> map = new HashMap<>();
                item.forEach((key, value) -> {
                    if (value != null) {
                        if (Date.class.isAssignableFrom(value.getClass())) {
                            Date ndate = new Date(((Date) value).getTime() - TIME_8_H);
                            map.put(key, ndate);
                        }
                    }
                });
                item.putAll(map);
            });
            String json = JsonHelper.toJsonStringWithoutNullAndSSS(result);
            e = JsonHelper.fromJson(type, json, GsonHelper.DATE_PATTERN_MILL);
        }
        return e;
    }

    /**
     * 查询总数
     *
     * @param sql 原始sql
     * @return 总数
     */
    public Integer queryCount(String sql) {
        int fromindex = sql.toLowerCase().indexOf(" from ");
        if (fromindex < 0) {
            throw new RuntimeException("sql" + sql + " has no from");
        }
        boolean isHasOrder = false;
        int orderbyindex = sql.toLowerCase().indexOf(" order ");
        if (orderbyindex > 0) {
            isHasOrder = !sql.toLowerCase().substring(orderbyindex).contains(")");
        }
        String countsql = sql;
        if (isHasOrder) {
            orderbyindex = countsql.toLowerCase().indexOf(" order ");
            countsql = countsql.substring(0, orderbyindex);
        }
        countsql = "select count(*) " + countsql.substring(fromindex);
        Map<String, Object> map = getJdbcTemplate().queryForMap(countsql);
        Integer[] count = new Integer[1];
        map.forEach((k, v) -> count[0] = Integer.valueOf(String.valueOf(v)));
        return count[0];
    }

}
