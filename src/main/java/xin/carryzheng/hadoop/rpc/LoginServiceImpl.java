package xin.carryzheng.hadoop.rpc;

/**
 * @author zhengxin
 * @date 2018-03-27 14:29:40
 */
public class LoginServiceImpl implements LoginService {


    @Override
    public String login(String username, String password) {

        return username + " login successfully.";
    }
}
