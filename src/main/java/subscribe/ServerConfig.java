package subscribe;

import lombok.Data;

/**
 * 模拟配置信息
 */
@Data
public class ServerConfig {
    private String dbUrl;
    private String dbUser;
    private String dbPwd;
}
