package cn.emay.elasticsearch.ann;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * ES字段注解
 * 
 * @author frank
 *
 */
@Target({ ElementType.FIELD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface EsField {

	/**
	 * 字段类型
	 */
	EsFieldType type();

}
