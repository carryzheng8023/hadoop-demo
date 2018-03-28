package xin.carryzheng.hadoop.rpc;

/**
 * @author zhengxin
 * @date 2018-03-27 14:29:29
 */
public interface LoginService {

    public static final long versionID = 1L;

    public String login(String username, String password);

}
