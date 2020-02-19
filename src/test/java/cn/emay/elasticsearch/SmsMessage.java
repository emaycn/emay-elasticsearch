package cn.emay.elasticsearch;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Date;

import cn.emay.elasticsearch.ann.EsField;
import cn.emay.elasticsearch.ann.EsFieldType;
import cn.emay.elasticsearch.ann.EsIndex;

/**
 * 短信信息
 * 
 * @author Frank
 *
 */
@EsIndex(name = "sms_message_")
public class SmsMessage implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * id 等同于 smsid
	 */
	@EsField(type = EsFieldType.LONG)
	private Long id;
	/**
	 * 手机
	 */
	@EsField(type = EsFieldType.KEYWORD)
	private String mobile;
	/**
	 * 扩展码
	 */
	@EsField(type = EsFieldType.KEYWORD)
	private String extendedCode;
	/**
	 * APP KEY
	 */
	@EsField(type = EsFieldType.KEYWORD)
	private String appKey;
	/**
	 * APP 扩展
	 */
	@EsField(type = EsFieldType.KEYWORD)
	private String appCode;
	/**
	 * 提交时间
	 */
	@EsField(type = EsFieldType.DATE)
	private Date submitTime;
	/**
	 * 内容
	 */
	@EsField(type = EsFieldType.TEXT)
	private String content;
	/**
	 * SMS ID
	 */
	@EsField(type = EsFieldType.KEYWORD)
	private String smsId;
	/**
	 * 接口编码
	 */
	@EsField(type = EsFieldType.KEYWORD)
	private String interfaceServiceNo;
	/**
	 * 批次号
	 */
	@EsField(type = EsFieldType.KEYWORD)
	private String batchNo;
	/**
	 * 客户IP
	 */
	@EsField(type = EsFieldType.KEYWORD)
	private String remoteIp;
	/**
	 * 通道ID
	 */
	@EsField(type = EsFieldType.INTEGER)
	private Long channelId;
	/**
	 * 运营商CODE
	 */
	@EsField(type = EsFieldType.KEYWORD)
	private String operatorCode;
	/**
	 * 省份Code
	 */
	@EsField(type = EsFieldType.KEYWORD)
	private String provinceCode;
	/**
	 * 城市
	 */
	@EsField(type = EsFieldType.KEYWORD)
	private String city;
	/**
	 * 计费条数
	 */
	@EsField(type = EsFieldType.INTEGER)
	private Integer cost;
	/**
	 * 状态：1-待发送，2-发送中，3-已发送，4-发送成功，5-发送失败
	 */
	@EsField(type = EsFieldType.INTEGER)
	private Integer state;
	/**
	 * 发送时间
	 */
	@EsField(type = EsFieldType.DATE)
	private Date sendTime;
	/**
	 * 状态报告返回时间
	 */
	@EsField(type = EsFieldType.DATE)
	private Date reportTime;
	/**
	 * 响应返回时间
	 */
	@EsField(type = EsFieldType.DATE)
	private Date responseTime;
	/**
	 * 运营商响应代码
	 */
	@EsField(type = EsFieldType.KEYWORD)
	private String responseCode;
	/**
	 * 运营商状态报告代码
	 */
	@EsField(type = EsFieldType.KEYWORD)
	private String reportCode;
	/**
	 * 最终的状态代码【自身定义】
	 */
	@EsField(type = EsFieldType.KEYWORD)
	private String resultCode;
	/**
	 * app计费
	 */
	@EsField(type = EsFieldType.DOUBLE)
	private BigDecimal charge;
	/**
	 * 应用单价
	 */
	@EsField(type = EsFieldType.DOUBLE)
	private BigDecimal price;
	/**
	 * 客户ID
	 */
	@EsField(type = EsFieldType.LONG)
	private Long clientId;
	/**
	 * 通道响应ID
	 */
	@EsField(type = EsFieldType.KEYWORD)
	private String channelResponseId;
	/**
	 * 通道状态报告Id
	 */
	@EsField(type = EsFieldType.KEYWORD)
	private String channelReportId;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getMobile() {
		return mobile;
	}

	public void setMobile(String mobile) {
		this.mobile = mobile;
	}

	public String getExtendedCode() {
		return extendedCode;
	}

	public void setExtendedCode(String extendedCode) {
		this.extendedCode = extendedCode;
	}

	public String getAppKey() {
		return appKey;
	}

	public void setAppKey(String appKey) {
		this.appKey = appKey;
	}

	public String getAppCode() {
		return appCode;
	}

	public void setAppCode(String appCode) {
		this.appCode = appCode;
	}

	public Date getSubmitTime() {
		return submitTime;
	}

	public void setSubmitTime(Date submitTime) {
		this.submitTime = submitTime;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public String getSmsId() {
		return smsId;
	}

	public void setSmsId(String smsId) {
		this.smsId = smsId;
	}

	public String getBatchNo() {
		return batchNo;
	}

	public void setBatchNo(String batchNo) {
		this.batchNo = batchNo;
	}

	public String getRemoteIp() {
		return remoteIp;
	}

	public void setRemoteIp(String remoteIp) {
		this.remoteIp = remoteIp;
	}

	public Long getChannelId() {
		return channelId;
	}

	public void setChannelId(Long channelId) {
		this.channelId = channelId;
	}

	public String getOperatorCode() {
		return operatorCode;
	}

	public void setOperatorCode(String operatorCode) {
		this.operatorCode = operatorCode;
	}

	public String getProvinceCode() {
		return provinceCode;
	}

	public void setProvinceCode(String provinceCode) {
		this.provinceCode = provinceCode;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public Integer getCost() {
		return cost;
	}

	public void setCost(Integer cost) {
		this.cost = cost;
	}

	public Integer getState() {
		return state;
	}

	public void setState(Integer state) {
		this.state = state;
	}

	public Date getSendTime() {
		return sendTime;
	}

	public void setSendTime(Date sendTime) {
		this.sendTime = sendTime;
	}

	public Date getReportTime() {
		return reportTime;
	}

	public void setReportTime(Date reportTime) {
		this.reportTime = reportTime;
	}

	public Date getResponseTime() {
		return responseTime;
	}

	public void setResponseTime(Date responseTime) {
		this.responseTime = responseTime;
	}

	public String getResponseCode() {
		return responseCode;
	}

	public void setResponseCode(String responseCode) {
		this.responseCode = responseCode;
	}

	public String getReportCode() {
		return reportCode;
	}

	public void setReportCode(String reportCode) {
		this.reportCode = reportCode;
	}

	public BigDecimal getCharge() {
		return charge;
	}

	public void setCharge(BigDecimal charge) {
		this.charge = charge;
	}

	public BigDecimal getPrice() {
		return price;
	}

	public void setPrice(BigDecimal price) {
		this.price = price;
	}

	public String getInterfaceServiceNo() {
		return interfaceServiceNo;
	}

	public void setInterfaceServiceNo(String interfaceServiceNo) {
		this.interfaceServiceNo = interfaceServiceNo;
	}

	public String getResultCode() {
		return resultCode;
	}

	public void setResultCode(String resultCode) {
		this.resultCode = resultCode;
	}

	public Long getClientId() {
		return clientId;
	}

	public void setClientId(Long clientId) {
		this.clientId = clientId;
	}

	public String getChannelResponseId() {
		return channelResponseId;
	}

	public void setChannelResponseId(String channelResponseId) {
		this.channelResponseId = channelResponseId;
	}

	public String getChannelReportId() {
		return channelReportId;
	}

	public void setChannelReportId(String channelReportId) {
		this.channelReportId = channelReportId;
	}
}
