package cn.emay.elasticsearch;

import cn.emay.elasticsearch.respoitory.PojoEsRepository;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xpack.sql.jdbc.EsDataSource;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author frank
 */
public class SmsMessageRepository extends PojoEsRepository<SmsMessage> {

    private final String ip;
    private final int port;
    private final String user;
    private final String password;

    private RestHighLevelClient client;
    private JdbcTemplate jdbc;

    public SmsMessageRepository(String ip, int port) {
        this(ip, port, null, null);
    }

    public SmsMessageRepository(String ip, int port, String user, String password) {
        this.ip = ip;
        this.port = port;
        this.user = user;
        this.password = password;
        this.init();
    }

    private void init() {
        EsDataSource datasource = new EsDataSource();
        datasource.setUrl("jdbc:es://http://" + ip + ":" + port);
        if (user != null && password != null) {
            Properties pro = new Properties();
            pro.setProperty("user", user);
            pro.setProperty("password", password);
            datasource.setProperties(pro);
        }
        jdbc = new JdbcTemplate(datasource);

        if (user != null && password != null) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
            client = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(ip, port, "http")).setHttpClientConfigCallback(httpClientBuilder -> {
                        httpClientBuilder.disableAuthCaching();
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }));
        } else {
            client = new RestHighLevelClient(RestClient.builder(new HttpHost(ip, port, "http")));
        }
    }

    // 这一步可以交给spring
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected RestHighLevelClient getClient() {
        return client;
    }

    @Override
    protected JdbcTemplate getJdbcTemplate() {
        return jdbc;
    }

    public void updateByQuery() {
        String indexName = "sms_message_201912";
        UpdateByQueryRequest request = new UpdateByQueryRequest(indexName);
        QueryBuilder qb = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("saveTime").gte("2019-12-15 00:00:00 000").lte("2019-12-15 23:59:59 999"));
        request.setQuery(qb);
        Map<String, Object> params = new HashMap<>();
        params.put("charge", BigDecimal.valueOf(0));
        String idOrCode = "if(ctx._source.channelCharge == null) {ctx._source.channelCharge = params.charge}";
        Script sc = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, idOrCode, params);
        request.setScript(sc);
        try {
            this.getClient().updateByQuery(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

}
