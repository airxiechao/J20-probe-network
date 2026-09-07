# J20-probe-network
简单的网络流量探针（支持 TCP/UDP/HTTP）

## 1. 准备环境
捕获流量的机器上安装：
- **jdk**: 11
- **pcap**
    - linux: libpcap
    - windows: WinPcap or Npcap in WinPcap API-compatible Mode

## 2. 编译
`mvn install`

## 3. 流量探针启动
将 `boot-probe-network/target`中的`j20-probe-network-boot.jar`和`lib` ，以及配置文件`probe-network.yml`复制到捕获流量的机器上。修改流量探针的配置文件：
- capture 捕获配置
    - ip 捕获设备IP
    - protocols 捕获协议，支持 TCP/UDP/HTTP
    - output 输出配置
      - print 是否打印到终端 
      - kafka 输出到 Kafka 配置
      - file 输出到文件配置

启动流量探针 `java -jar j20-probe-network-boot.jar`



