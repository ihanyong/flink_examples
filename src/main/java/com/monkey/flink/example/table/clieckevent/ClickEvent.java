package com.monkey.flink.example.table.clieckevent;

/**
 * ClickEvent
 *
 * @author yong.han
 * 2019/3/7
 */
public class ClickEvent {
    private String user;
    private long cTime;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public long getcTime() {
        return cTime;
    }

    public void setcTime(long cTime) {
        this.cTime = cTime;
    }
}