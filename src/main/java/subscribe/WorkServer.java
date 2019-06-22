package subscribe;

import com.alibaba.fastjson.JSON;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;

/**
 * 工作服务器
 * 1、向zookeeper注册自己
 * 2、监听config配置文件是否改变
 */
public class WorkServer {
    private ZkClient zkClient = null;
    //服务要注册的路径
    private String serverPath;
    //监听的配置文件路径
    private String configPath;
    //服务器内部保存的配置文件
    private ServerConfig serverConfig;
    //服务器内部数据信息
    private ServerData serverData;
    //监听事件：监听配置文件信息的变化
    private IZkDataListener dataListener;

    public WorkServer(String serverPath, String configPath, ServerData serverData, ServerConfig serverConfig, ZkClient zkClient) {
        this.serverPath = serverPath;
        this.configPath = configPath;
        this.serverData = serverData;
        this.serverConfig = serverConfig;
        //初始化监听事件
        this.dataListener = new IZkDataListener() {
            public void handleDataChange(String dataPath, Object data) throws Exception {
                //反序列化为serverConfig对象
                String serverConfigStr = new String((byte[])data);
                ServerConfig newConfig = JSON.parseObject(serverConfigStr, ServerConfig.class);
                //调用更新方法
                updateSeverConfig(newConfig);
                System.out.println("update config, now is:" + newConfig.toString());
            }

            public void handleDataDeleted(String s) throws Exception {
            }
        };
    }


    public void start() {
        System.out.println("work server start...");
        initRunning();
    }

    public void stop() {
        System.out.println("work server stop...");
        zkClient.unsubscribeDataChanges(configPath, dataListener);

    }

    private void initRunning() {
        //向servers节点下注册自己（临时节点）
        registMe();
        //注册配置文件的监听事件
        zkClient.subscribeDataChanges(configPath, dataListener);
    }


    /**
     * 向zookeeper注册自己
     */
    public void registMe() {
        String mePath = this.serverPath.concat("/").concat(serverData.getAddress());
        try {
            zkClient.createEphemeral(mePath, JSON.toJSONString(serverData).getBytes());
        } catch (ZkNoNodeException e) {
            //说明serverPath没有被创建
            zkClient.createPersistent(serverPath, true);
            registMe();
        }

    }

    public void updateSeverConfig(ServerConfig newConfig) {
        this.serverConfig = newConfig;
    }
}
