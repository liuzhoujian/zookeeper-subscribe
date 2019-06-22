package subscribe;

import lombok.Data;

/**
 * 服务器内部数据信息
 */
@Data
public class ServerData {
    private String address;
    private Long id;
    private String name;
}
