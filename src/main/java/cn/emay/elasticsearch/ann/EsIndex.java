package cn.emay.elasticsearch.ann;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * ES 索引(表)的注解
 * 
 * @author frank
 *
 */
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface EsIndex {

	/**
	 * 索引名字(前缀)<br/>
	 */
	String name();

	/**
	 * 分片数
	 */
	int shards() default 1;

	/**
	 * 副本数
	 */
	int replicas() default 0;

}
