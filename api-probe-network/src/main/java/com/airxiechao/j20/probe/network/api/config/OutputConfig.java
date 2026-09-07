package com.airxiechao.j20.probe.network.api.config;

import lombok.Data;

/**
 * 输出配置
 */
@Data
public class OutputConfig {
    /**
     * Kafka 配置
     */
    private KafkaConfig kafka;

    /**
     * 输出文件配置
     */
    private FileConfig file;

    /**
     * 打印到终端
     */
    private Boolean print;
}
