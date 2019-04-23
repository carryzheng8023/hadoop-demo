package xin.carryzheng.hadoop.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;

/**
 * @author zhengxin
 * @date 2018-04-08 16:13:35
 */
public class PhoneNumToArea extends UDF {

    private static HashMap<String, String> areaMap = new HashMap<>();

    static {

        areaMap.put("1388", "beijing");
        areaMap.put("1399", "tianjin");
        areaMap.put("1366", "nanjing");
    }

    public String evaluate(String phoneNum){

        String result = areaMap.get(phoneNum.substring(0, 4));

        result = result == null ? (phoneNum + "\t" + "Mars") : (phoneNum + "\t" + result);

        return result;


    }

}
