package com.wwware.flink.test.stock;

import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

/**
 * MysqlSink
 *
 * @author yong.han
 * 2019/1/24
 */
public class MysqlSink extends RichSinkFunction<Tuple3<String, Long, TimeWindow>>  {

    DataSource dataSource;
    String desc_;


    @Override
    public void open(Configuration parameters) throws Exception {

        System.out.println("open MysqlSink");
        super.open(parameters);
        MysqlConnectionPoolDataSource mpds = new MysqlConnectionPoolDataSource();
        mpds.setUrl("jdbc:mysql://localhost:3306/flink_data?useUnicode=true&characterEncoding=utf-8&useSSL=false&allowMultiQueries=true");
        mpds.setUser("root");
        mpds.setPassword("root");

        dataSource = mpds;

        desc_ = new StringBuilder()
                .append(this.toString()).append(":::")
                .toString();

    }

    public String desc() {
        return new StringBuilder(desc_)
                .append(Thread.currentThread().getId()).append(":::")
                .append(Thread.currentThread().getName()).append(":::")
                .toString();
    }


    @Override
    public void close() throws Exception {
        System.out.println("close MysqlSink");

        super.close();
//        this.dataSource
    }

    @Override
    public void invoke(Tuple3<String, Long, TimeWindow> value, Context context) throws Exception {

        System.out.println("mysqlSink.invoke()");
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);


            PreparedStatement stat = conn.prepareStatement("insert into res_tbl_updates (time_window_end, table_name, update_times, sink_thread) values (?, ?, ?, ?)");
            stat.setTimestamp(1, new Timestamp(value.f2.getEnd()));
            stat.setString(2, value.f0);
            stat.setLong(3, value.f1);
            stat.setString(4, desc());

            stat.executeUpdate();

            conn.commit();
            System.out.println("insert into res_tbl_updates");
        }
    }
}
