package com.monkey.flink.example.table.clieckevent;

import java.sql.Timestamp;

/**
 * ClickReport
 *
 * @author yong.han
 * 2019/3/7
 */
public class ClickReport {
    private Timestamp windowEnd;
    private String user;
    private Long clickCount;


    public Timestamp getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Timestamp windowEnd) {
        this.windowEnd = windowEnd;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Long getClickCount() {
        return clickCount;
    }

    public void setClickCount(Long clickCount) {
        this.clickCount = clickCount;
    }

    @Override
    public String toString() {
        return "ClickReport{" +
                "windowEnd=" + windowEnd +
                ", user='" + user + '\'' +
                ", clickCount=" + clickCount +
                '}';
    }
}