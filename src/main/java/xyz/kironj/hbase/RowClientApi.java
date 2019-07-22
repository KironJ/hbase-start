package xyz.kironj.hbase;

import com.sun.javafx.binding.StringFormatter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import xyz.kironj.hbase.common.RandomValue;
import xyz.kironj.hbase.entities.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RowClientApi {

    private static final String TABLE_NAME = "kirin_user_table";

    public static void main(String[] args) throws IOException {


        for (int i = 0; i < 20; i++) {
            long cTime = System.currentTimeMillis();
            System.out.println("Long.MAX_VALUE - " +  cTime + " = " + (Long.MAX_VALUE - cTime));
        }

        // 1563521606691
//        runProcedureReversed();
//        scanReversed();
    }


    /**
     *
     * 1. 100 -1 => 99
     * 2. 100 -2 => 98
     * 3. 100 -3 => 97
     * 4. 100 -4 => 96
     * 5. 100 -5 => 95
     * 6. 100 -6 => 94
     *
     * @throws IOException IOException
     *
     */
    private static void scanReversed() throws IOException {

        Connection connection = initHbase();
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        Scan scan = new Scan();
        scan.setReversed(true);
        scan.setStartRow(Bytes.toBytes(Long.MAX_VALUE - 1563521606691L + ""));
        scan.setStopRow(Bytes.toBytes (Long.MAX_VALUE - 1563521607691L + ""));
        ResultScanner results = table.getScanner(scan);
        for(Result result: results) {
            System.out.println(result);
        }
    }

    /**
     * 按时间戳倒叙插入 即 Long.max-timestamp.
     *
     * @throws IOException  IO异常
     */
    private static void runProcedureReversed() throws IOException {
        Connection connection = initHbase();
        TableName tableName = TableName.valueOf(TABLE_NAME);
        String[] cols = new String[]{"cf1"};

        // 建表
        Admin admin = connection.getAdmin();
        if (admin.tableExists(tableName)) {
            System.out.println("表已存在, 无需建表.");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }

        // 插入数据
        for (int i = 0; i < 5000; i++) {
            Map<String, String> prof = RandomValue.getProfile();
            long currentTime = System.currentTimeMillis();
            System.out.println("currentTime: " + currentTime);
            User user = new User(String.valueOf(Long.MAX_VALUE - currentTime),
                    prof.get(RandomValue.ATTR.Name.name()),
                    prof.get(RandomValue.ATTR.Gender.name()),
                    RandomValue.getNum(16, 80),
                    prof.get(RandomValue.ATTR.Phone.name()),
                    prof.get(RandomValue.ATTR.Email.name())
            );

            Put put = new Put((user.getId()).getBytes());
            //参数：1.列族名  2.列名  3.值
            byte[] bytes = cols[0].getBytes();
            put.addColumn(bytes, "username".getBytes(), user.getUsername().getBytes());
            put.addColumn(bytes, "age".getBytes(), Bytes.toBytes(user.getAge()));
            put.addColumn(bytes, "gender".getBytes(), user.getGender().getBytes());
            put.addColumn(bytes, "phone".getBytes(), user.getPhone().getBytes());
            put.addColumn(bytes, "email".getBytes(), user.getEmail().getBytes());
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            table.put(put);
        }

        // 查找数据
        List<User> list = new ArrayList<User>();
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        ResultScanner results = table.getScanner(new Scan());
        for (Result result : results) {
//            String id = new String(result.getRow());
            User u = new User();
            for (Cell cell : result.rawCells()) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                //String family =  Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                u.setId(row);
                if (colName.equals("username")) {
                    u.setUsername(value);
                } else if (colName.equals("age")) {
                    u.setAge(Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                } else if (colName.equals("gender")) {
                    u.setGender(value);
                } else if (colName.equals("phone")) {
                    u.setPhone(value);
                } else if (colName.equals("email")) {
                    u.setEmail(value);
                }
            }
            list.add(u);
        }
        for (User user: list) {
            System.out.println("user: " + user);
        }
    }

    /**
     * 插入数据.
     *
     * @throws IOException      IO异常
     */
    private static void runProcedure() throws IOException {
        Connection connection = initHbase();
        TableName tableName = TableName.valueOf(TABLE_NAME);
        String[] cols = new String[]{"cf1"};

        // 建表
        Admin admin = connection.getAdmin();
        if (admin.tableExists(tableName)) {
            System.out.println("表已存在, 无需建表.");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            for (String col : cols) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
        }

        // 插入数据
        for (int i = 0; i < 5000; i++) {
            Map<String, String> prof = RandomValue.getProfile();

            User user = new User(
                    "u_" + RandomValue.getNum(4, 10) + i,
                    prof.get(RandomValue.ATTR.Name.name()),
                    prof.get(RandomValue.ATTR.Gender.name()),
                    RandomValue.getNum(16, 80),
                    prof.get(RandomValue.ATTR.Phone.name()),
                    prof.get(RandomValue.ATTR.Email.name())
            );


            Put put = new Put((user.getId()).getBytes());
            //参数：1.列族名  2.列名  3.值
            byte[] bytes = cols[0].getBytes();
            put.addColumn(bytes, "username".getBytes(), user.getUsername().getBytes());
            put.addColumn(bytes, "age".getBytes(), Bytes.toBytes(user.getAge()));
            put.addColumn(bytes, "gender".getBytes(), user.getGender().getBytes());
            put.addColumn(bytes, "phone".getBytes(), user.getPhone().getBytes());
            put.addColumn(bytes, "email".getBytes(), user.getEmail().getBytes());
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            table.put(put);
        }

        // 查找数据
        List<User> list = new ArrayList<User>();
        Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
        ResultScanner results = table.getScanner(new Scan());
        for (Result result : results) {
//            String id = new String(result.getRow());
            User u = new User();
            for (Cell cell : result.rawCells()) {
                String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                //String family =  Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                u.setId(row);
                if (colName.equals("username")) {
                    u.setUsername(value);
                } else if (colName.equals("age")) {
                    u.setAge(Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                } else if (colName.equals("gender")) {
                    u.setGender(value);
                } else if (colName.equals("phone")) {
                    u.setPhone(value);
                } else if (colName.equals("email")) {
                    u.setEmail(value);
                }
            }
            list.add(u);
        }
        for (User user: list) {
            System.out.println("user: " + user);
        }
    }

    //连接集群
    private static Connection initHbase() throws IOException {
        // 配置内容参见hbase-site.xml文件.
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "node01.test.bigdata.hbh,node02.test.bigdata.hbh,node03.test.bigdata.hbh");
        configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
        // 集群配置↓
        return ConnectionFactory.createConnection(configuration);
    }

}