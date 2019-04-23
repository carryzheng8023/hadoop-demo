package xin.carryzheng.hadoop.hive;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author zhengxin
 * @date 2019-04-08 11:16:42
 */
public class Lower extends UDF {

    public String evaluate(final String s){

        if(null == s)
            return null;

        return s.toLowerCase();
    }

}
