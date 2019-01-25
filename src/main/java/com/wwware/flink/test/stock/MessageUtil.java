package com.wwware.flink.test.stock;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.position.EntryPosition;

import java.util.*;

/**
 * Message对象解析工具类
 *
 * @author rewerma 2018-8-19 下午06:14:23
 * @version 1.0.0
 */
public class MessageUtil {

//    public static List<Dml> parse4Dml(String destination, Message message, EntryPosition nextPosition) {
    public static List<Dml> parse4Dml(String destination, Message message) {

        if (message == null) {
            return null;
        }
        List<CanalEntry.Entry> entries = message.getEntries();
        List<Dml> dmls = new ArrayList<Dml>(entries.size());
        for (CanalEntry.Entry entry : entries) {

            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN
                    || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChange;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            CanalEntry.EventType eventType = rowChange.getEventType();

            final Dml dml = new Dml();
            dml.setDestination(destination);
            dml.setDatabase(entry.getHeader().getSchemaName());
            dml.setTable(entry.getHeader().getTableName());
            dml.setType(eventType.toString());
            dml.setEs(entry.getHeader().getExecuteTime());
            dml.setTs(System.currentTimeMillis());

            dml.setSql(rowChange.getSql());
            dmls.add(dml);
            List<Map<String, Object>> data = new ArrayList<>();
            List<Map<String, Object>> old = new ArrayList<>();

            if (!rowChange.getIsDdl()) {
                Set<String> updateSet = new HashSet<>();
                for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                    if (eventType != CanalEntry.EventType.INSERT && eventType != CanalEntry.EventType.UPDATE
                            && eventType != CanalEntry.EventType.DELETE) {
                        continue;
                    }

                    Map<String, Object> row = new LinkedHashMap<>();
                    List<CanalEntry.Column> columns;

                    if (eventType == CanalEntry.EventType.DELETE) {
                        columns = rowData.getBeforeColumnsList();
                    } else {
                        columns = rowData.getAfterColumnsList();
                    }

                    for (CanalEntry.Column column : columns) {
                        row.put(column.getName(),
                                JdbcTypeUtil.typeConvert(column.getName(),
                                        column.getValue(),
                                        column.getSqlType(),
                                        column.getMysqlType()));
                        // 获取update为true的字段
                        if (column.getUpdated()) {
                            updateSet.add(column.getName());
                        }
                    }
                    if (!row.isEmpty()) {
                        data.add(row);
                    }

                    if (eventType == CanalEntry.EventType.UPDATE) {
                        Map<String, Object> rowOld = new LinkedHashMap<>();
                        for (CanalEntry.Column column : rowData.getBeforeColumnsList()) {
                            if (updateSet.contains(column.getName())) {
                                rowOld.put(column.getName(),
                                        JdbcTypeUtil.typeConvert(column.getName(),
                                                column.getValue(),
                                                column.getSqlType(),
                                                column.getMysqlType()));
                            }
                        }
                        // update操作将记录修改前的值
                        if (!rowOld.isEmpty()) {
                            old.add(rowOld);
                        }
                    }
                }
                if (!data.isEmpty()) {
                    dml.setData(data);
                }
                if (!old.isEmpty()) {
                    dml.setOld(old);
                }
            }
        }

        // get the next position to process
//        if (entries.size() > 0) {
//            CanalEntry.Header lastHeader = entries.get(entries.size() - 1).getHeader();
//            String fileName = lastHeader.getLogfileName();
//            long lastOffset = lastHeader.getLogfileOffset();
//            long lastLen = lastHeader.getEventLength();
//            long nextOffset = lastOffset + lastLen;
//
//            nextPosition.setJournalName(fileName);
//            nextPosition.setPosition(nextOffset);
//            nextPosition.setServerId(lastHeader.getServerId());
//        }

        return dmls;
    }


}
