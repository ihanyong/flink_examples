package com.monkey.flink.example.table.clieckevent;

import java.sql.Timestamp;

/**
 * ClickEvent
 *
 * @author yong.han
 * 2019/3/7
 */
public class ClickEvent {
    private String user;
    private Timestamp cTime;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Timestamp getcTime() {
        return cTime;
    }

    public void setcTime(Timestamp cTime) {
        this.cTime = cTime;
    }
}