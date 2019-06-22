package subscribe;

import com.alibaba.fastjson.JSON;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;

import java.util.List;

/**
 * 管理服务器
 * 1、管理工作服务器列表
 * 2、监听command节点数据
 */
public class ManageServer {
    private ZkClient zkClient;
    private String configPath;
    private String serversPath;
    private String commandPath;
    //监听工作服务器列表
    private IZkChildListener childListener;
    //监听command节点数据
    private IZkDataListener dataListener;
    //服务器配置信息
    private ServerConfig serverConfig;
    private List<String> workServerList;

    public ManageServer(String configPath, String commandPath, String serversPath, ZkClient zkClient, ServerConfig config) {
        this.configPath = configPath;
        this.commandPath = commandPath;
        this.serversPath = serversPath;
        this.zkClient = zkClient;
        this.serverConfig = config;
        this.childListener = new IZkChildListener() {
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                //更新管理服务区中的工作服务器列表
                workServerList = currentChilds;
                System.out.println("managerServer中的工作服务器列表最新为：" + currentChilds.toString());
            }
        };

        this.dataListener = new IZkDataListener() {
            public void handleDataChange(String parentPath, Object command) throws Exception {
                //command数据发生变化
                String cmdType = new String((byte[])command);
                //执行cmd
                exeCmd(cmdType);
            }

            public void handleDataDeleted(String s) throws Exception {}
        };
    }

    /**1、list
     * 2、create
     * 3、modify
     *
     * @param cmdType
     */
    private void exeCmd(String cmdType) {
        if(COMMAND.LIST.cmdType.equals(cmdType)) {
            execList();
        } else if(COMMAND.CREATE.cmdType.equals(cmdType)) {
            execCreate();
        } else if(COMMAND.MODIFY.cmdType.equals(cmdType)) {
            execModify();
        } else {
            System.out.println("error command:" + cmdType);
        }
    }

    private void execList() {
        System.out.println(workServerList);
    }

    private void execCreate() {
        //节点不存在
        if(!zkClient.exists(configPath)) {
            //先创建config节点
            try {
                zkClient.createPersistent(configPath, JSON.toJSONString(serverConfig).getBytes()); //将对象转为二进制数值保存在节点中
            } catch (ZkNodeExistsException e) {
                zkClient.writeData(configPath, JSON.toJSONString(serverConfig).getBytes());
            } catch (ZkNoNodeException e) {
                String parentPath = configPath.substring(0, configPath.lastIndexOf("/"));
                zkClient.createPersistent(parentPath, true);
                execCreate();
            }
        }
    }

    private void execModify() {
        serverConfig.setDbUser(serverConfig.getDbUser() + "_modify");

        try {
            zkClient.writeData(configPath, JSON.toJSONString(serverConfig).getBytes());
        } catch (ZkNoNodeException e) {
            execCreate();
        }

    }

    public void start() {
        initRunning();
    }

    public void stop() {
        zkClient.unsubscribeChildChanges(serversPath, childListener);
        zkClient.unsubscribeDataChanges(commandPath, dataListener);
    }

    private void initRunning() {
        //监听servers列表变化
        zkClient.subscribeChildChanges(serversPath, childListener);
        //监听command数据变化
        zkClient.subscribeDataChanges(commandPath, dataListener);
    }

    private enum COMMAND {
        LIST("list"),
        CREATE("create"),
        MODIFY("modify");

        private String cmdType;
        COMMAND(String cmdType) {
            this.cmdType = cmdType;
        }
    }

}
