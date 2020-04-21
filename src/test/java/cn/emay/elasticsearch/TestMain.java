package cn.emay.elasticsearch;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;

import cn.emay.elasticsearch.item.EsInsertItem;

public class TestMain {

	public static void main(String[] args) {

		String suffix = "20200101";

		SmsMessageRepository messagerep = new SmsMessageRepository();

		String sql = "select reportCode,count(*) as count from sms_message_20200101 where batchNo = '20200101111230009'  group by reportCode   limit 1";
		List<Map<String, Object>> queryForList = messagerep.getJdbcTemplate().queryForList(sql, new Object[] {});
		for (Map<String, Object> map : queryForList) {
			System.out.println(map);

		}
		messagerep.close();
	}

	public static void testUpdateByScript(SmsMessageRepository messagerep, String suffix) {
		String id = "12039283920192";
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("appCode", "0987980");
		params.put("appKey", "sdfsdfsdf23fsdfsefsf");
		String idOrCode = "if(ctx._source.id != params.appKey) {ctx._source.appCode = params.appCode}";
		Script sc = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, idOrCode, params);
		messagerep.update(suffix, id, sc);
	}

	public static void testUpdateByParams(SmsMessageRepository messagerep, String suffix) {
		String id = "12039283920192";
		SmsMessage sms = new SmsMessage();
		sms.setAppCode("12345");
		sms.setSubmitTime(new Date());
		messagerep.update(suffix, id, sms);
	}

	public static void testSave(SmsMessageRepository messagerep, String suffix) {
		List<EsInsertItem<SmsMessage>> list = new ArrayList<>();
		for (int j = 0; j < 100; j++) {
			// 模拟
			String enj = "00000" + j;
			String smsId = suffix + "123456789" + enj.substring(enj.length() - 3, enj.length());
			SmsMessage sms = new SmsMessage();
			sms.setAppCode("852");
			sms.setAppKey("7da59c3e511c4b9e");
			sms.setBatchNo(suffix + "11123000" + j);
			sms.setChannelId(1L);
			sms.setChannelReportId("2019001" + j);
			sms.setChannelResponseId("2019002" + j);
			sms.setCharge(BigDecimal.valueOf(0.03d));
			sms.setCity("北京");
			sms.setClientId(1L);
			sms.setContent("你好，这里是北京，您的编号是：" + suffix + j);
			sms.setCost(1);
			sms.setExtendedCode("012");
			sms.setId(Long.valueOf(smsId));
			sms.setInterfaceServiceNo("111");
			sms.setOperatorCode("CM");
			sms.setPrice(BigDecimal.valueOf(0.03d));
			sms.setProvinceCode("11");
			sms.setRemoteIp("127.0.0.1");
			sms.setReportCode("DELIVER");
			sms.setReportTime(new Date());
			sms.setResponseCode("DELIVER");
			sms.setResponseTime(new Date());
			sms.setResultCode("DELIVER");
			sms.setSendTime(new Date());
			sms.setSmsId(smsId);
			sms.setState(1);
			sms.setSubmitTime(new Date());
			sms.setMobile("18500244545");
			EsInsertItem<SmsMessage> mes = new EsInsertItem<SmsMessage>(sms.getId(), sms);
			list.add(mes);
		}
		messagerep.save(suffix, list);
	}

}
