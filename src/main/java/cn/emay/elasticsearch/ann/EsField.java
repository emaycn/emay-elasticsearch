package cn.emay.elasticsearch.ann;

import java.lang.annotation.*;

/**
 * ES字段注解
 *
 * @author frank
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface EsField {

    /**
     * 字段类型
     */
    EsFieldType type();

}
