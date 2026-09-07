package com.airxiechao.j20.probe.network.capture;

import com.airxiechao.j20.probe.common.kafka.KafkaAdmin;
import com.airxiechao.j20.probe.common.kafka.KafkaClient;
import com.airxiechao.j20.probe.network.api.config.CaptureConfig;
import com.airxiechao.j20.probe.network.api.config.OutputConfig;
import com.airxiechao.j20.probe.network.api.config.ProbeCaptureConfig;
import com.airxiechao.j20.probe.network.api.pojo.tree.LayerNode;
import com.airxiechao.j20.probe.network.api.pojo.tree.TreeNode;
import com.airxiechao.j20.probe.network.kafka.KafkaManager;
import com.airxiechao.j20.probe.network.protocol.IProtocol;
import com.airxiechao.j20.probe.network.protocol.ProtocolFactory;
import com.alibaba.fastjson2.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.pcap4j.core.*;
import org.pcap4j.packet.Packet;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.*;

/**
 * 捕获器
 */
@Slf4j
public class ProtocolCapture {
    /**
     * 捕获包长度
     */
    private static final int SNAP_LEN = 65536;

    /**
     * 捕获超时
     */
    private static final int READ_TIMEOUT = 0;

    /**
     * 捕获处理器列表
     */
    private List<PcapHandle> handles = new ArrayList<>();

    public ProtocolCapture() {

    }

    /**
     * 启动
     * @throws Exception
     */
    public void start() throws Exception {
        // 读取配置文件
        ProbeCaptureConfig config;
        try{
            config = ProbeCaptureConfigFactory.getInstance().get();
            log.info("读取配置文件：{}", JSON.toJSONString(config));
        }catch (Exception e){
            throw new Exception("读取配置文件发生错误", e);
        }

        // 启动每一个捕获
        List<CaptureConfig> captures = config.getCaptures();
        for (CaptureConfig captureConfig : captures) {
            startOneAsync(captureConfig);
        }
    }

    /**
     * 停止
     */
    public void stop(){
        log.info("停止捕获");
        for (PcapHandle handle : handles) {
            if(null != handle){
                handle.close();
            }
        }
    }

    /**
     * 异步启动单个捕获处理器
     * @param captureConfig 捕获配置
     * @throws Exception 捕获异常
     */
    private void startOneAsync(CaptureConfig captureConfig) throws Exception {
        Set<String> filterProtocols = captureConfig.getProtocols();
        String interfaceIp = captureConfig.getIp();
        OutputConfig outputConfig = captureConfig.getOutput();

        // 裁剪协议树
        Set<String> protocols = new HashSet<>();
        for (String protocol : filterProtocols) {
            List<String> path = ProtocolFactory.getInstance().getProtocolPath(protocol);
            protocols.addAll(path);
        }

        TreeNode<Class<?>> protocolTree = ProtocolFactory.getInstance().getProtocolTree();
        TreeNode<Class<?>> trimmedProtocolTree = trimProtocolTree(protocolTree, protocols);

        if(null == trimmedProtocolTree){
            throw new Exception("协议为空");
        }

        PcapNetworkInterface nif = Pcaps.getDevByAddress(InetAddress.getByName(interfaceIp));
        if(null == nif){
            throw new Exception("设备未找到");
        }

        // 检查Kafka输出队列
        KafkaClient kafkaClient;
        if(null != outputConfig && null != outputConfig.getKafka()){
            String bootstrapServers = outputConfig.getKafka().getBootstrapServers();
            String dstTopic = outputConfig.getKafka().getTopic();
            Integer timeoutMs = outputConfig.getKafka().getTimeoutMs();
            try(KafkaAdmin kafkaAdmin = KafkaManager.getInstance().getAdmin(bootstrapServers, timeoutMs)){
                if(!kafkaAdmin.hasTopic(dstTopic)){
                    kafkaAdmin.createTopic(dstTopic, 1, 1);
                }
            }catch (Exception e){
                throw new Exception("检查 Kafka 输出队列发送错误", e);
            }

            kafkaClient = KafkaManager.getInstance().getClient(bootstrapServers, dstTopic);
        }else{
            kafkaClient = null;
        }

        // 检查文件输出文件夹
        String fileDir;
        if(null != outputConfig && null != outputConfig.getFile()){
            String dir = outputConfig.getFile().getDir();
            if(StringUtils.isBlank(dir)){
                throw new Exception("输出文件夹配置为空");
            }

            // 创建输出日志文件夹
            File dirFile = new File(dir);
            if(!dirFile.exists()){
                dirFile.mkdirs();
            }

            fileDir = dir;
        }else{
            fileDir = null;
        }

        // 是否打印到终端
        boolean isPrint;
        if(null != outputConfig && null != outputConfig.getPrint()){
            isPrint = outputConfig.getPrint();
        }else{
            isPrint = false;
        }

        log.info("开始捕获设备：{}，协议：{}", nif, filterProtocols);

        PcapHandle handle = nif.openLive(SNAP_LEN, PcapNetworkInterface.PromiscuousMode.PROMISCUOUS, READ_TIMEOUT);
        handles.add(handle);

        // 启动线程
        Thread thread = new Thread(() -> {
            try {
                handle.loop(0, new PacketListener() {
                    private LocalDate lastDate;
                    private PrintWriter fileWriter;

                    @Override
                    public void gotPacket(Packet packet) {
                        // 解析
                        List<Object> outputs = null;
                        try {
                            outputs = traverseProtocolTree(new LayerNode(packet, null), trimmedProtocolTree, filterProtocols);
                        } catch (Exception e) {
                            log.error("解析包发生错误", e);
                        }

                        // 输出
                        if(null != outputs){
                            for (Object output : outputs) {
                                String jsonString = JSON.toJSONString(output);
                                log.debug("捕获：{}", jsonString);

                                // 输出到 Kafka
                                if(null != kafkaClient) {
                                    try {
                                        kafkaClient.produce(null, jsonString);
                                    }catch (Exception e){
                                        log.error("输出 Kafka 发生错误", e);
                                    }
                                }

                                // 输出到文件
                                if(null != fileDir){
                                    LocalDate currentDate = LocalDate.now();

                                    try{
                                        PrintWriter writer = getFileWriter(fileDir, currentDate);
                                        writer.println(jsonString);
                                        writer.flush();
                                    }catch (Exception e){
                                        log.error("输出文件发生错误", e);
                                    }
                                }

                                // 输出到终端
                                if(isPrint){
                                    System.out.println(jsonString);
                                }
                            }
                        }
                    }

                    private PrintWriter getFileWriter(String dir, LocalDate date) throws IOException {
                        if(null == fileWriter){
                            fileWriter = new PrintWriter(new FileWriter(String.format("%s/probe-network-%s.log", dir, date), StandardCharsets.UTF_8, true));
                        }else if(!lastDate.equals(date)){
                            fileWriter.close();

                            fileWriter = new PrintWriter(new FileWriter(String.format("%s/probe-network-%s.log", dir, date), StandardCharsets.UTF_8, true));
                        }

                        lastDate = date;

                        return fileWriter;
                    }
                });
            } catch (Exception e) {
                log.error("捕获设备[{}]发生错误", nif, e);
            }
        });
        thread.start();
    }

    /**
     * 修剪协议树
     * @param node 协议树节点
     * @param names 协议名称集合
     * @return 修建后的协议树
     */
    private TreeNode<Class<?>> trimProtocolTree(TreeNode<Class<?>> node, Set<String> names){
        if(!names.contains(node.getName())){
            return null;
        }

        List<TreeNode<Class<?>>> newChildren = new ArrayList<>();
        for (TreeNode<Class<?>> child : node.getChildren()) {
            TreeNode<Class<?>> newChild = trimProtocolTree(child, names);
            if(null != newChild){
                newChildren.add(newChild);
            }
        }

        TreeNode<Class<?>> newNode = new TreeNode<>();
        newNode.setName(node.getName());
        newNode.setNode(node.getNode());
        newNode.setChildren(newChildren);

        return newNode;
    }

    /**
     * 遍历协议树
     * @param outerPayload 外层对象
     * @param node 协议树节点
     * @param filterProtocols 协议集合
     * @return 输出数据列表
     * @throws Exception 遍历异常
     */
    private List<Object> traverseProtocolTree(LayerNode outerPayload, TreeNode<Class<?>> node, Set<String> filterProtocols) throws Exception {
        List<Object> outputs = new ArrayList<>();

        Class<?> cls = node.getNode();
        Object obj = cls.getDeclaredConstructor().newInstance();

        if(obj instanceof IProtocol){
            IProtocol protocol = (IProtocol) obj;

            // 解析
            if(!protocol.parse(outerPayload)){
                return outputs;
            }

            // 输出
            if(filterProtocols.contains(node.getName())){
                Object output = protocol.getOutput();
                if(null != output){
                    outputs.add(output);
                }
            }

            // 下一步解析
            LayerNode payload = protocol.getPayload();
            if(null == payload){
                return outputs;
            }

            for (TreeNode<Class<?>> child : node.getChildren()) {
                List<Object> childOutputs = traverseProtocolTree(payload, child, filterProtocols);
                outputs.addAll(childOutputs);
            }
        }

        return outputs;
    }
}
