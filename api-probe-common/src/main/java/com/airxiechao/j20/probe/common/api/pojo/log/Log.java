package com.airxiechao.j20.probe.common.api.pojo.log;

import com.alibaba.fastjson2.JSONObject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 输出日志
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Log {

    /**
     * 数据内容
     */
    private JSONObject data;

    /**
     * 时间戳
     */
    private Long timestamp;

    /**
     * 构造函数
     * @param data 内容
     * @param timestamp 时间戳
     */
    public Log(JSONObject data, long timestamp) {
        this.data = data;
        this.timestamp = timestamp;
    }

    /**
     * 转字符串描述
     * @return 描述
     */
    @Override
    public String toString() {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return String.format("(timestamp=%s, log=%s)",
                sf.format(new Date(timestamp)), data.toJSONString());
    }

}
