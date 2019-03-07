package com.monkey.flink.example.table.clieckevent;

/**
 * ClickReport
 *
 * @author yong.han
 * 2019/3/7
 */
public class ClickReport {
    private String user;
    private Long clickCount;

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
                "user='" + user + '\'' +
                ", clickCount=" + clickCount +
                '}';
    }
}