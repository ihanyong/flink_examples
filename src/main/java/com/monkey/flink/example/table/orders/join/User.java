package com.monkey.flink.example.table.orders.join;

import lombok.*;

import java.sql.Timestamp;

/**
 * User
 *
 * @author yong.han
 * 2019/3/19
 */
@Data
@RequiredArgsConstructor()
@NoArgsConstructor
public class User {
    @NonNull
    private String id;
    @NonNull
    private String name;
    @NonNull
    private long updateTime;
    private Timestamp updateTimestamp;
}
