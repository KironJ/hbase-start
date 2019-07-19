package xyz.kironj.hbase;

import xyz.kironj.hbase.common.RandomValue;
import xyz.kironj.hbase.entities.User;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static xyz.kironj.hbase.common.RandomValue.getProfile;

public class ClientApi {

    private static Admin admin;

    private static final String TABLE_NAME = "kirin_user_table";

    private static Connection HBASE_CONNECTION = null;

    public static void main(String[] args) throws IOException {
        runExercise();
    }

    public static void runExercise() throws IOException {
            HBASE_CONNECTION = initHbase();
            TableName tableName = TableName.valueOf(TABLE_NAME);
            String[] cols = new String[]{"cf1"};
            admin = HBASE_CONNECTION.getAdmin();
            if (admin.tableExists(tableName)) {
                System.out.println("表已存在！");
            } else {
                HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
                for (String col : cols) {
                    HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(col);
                    hTableDescriptor.addFamily(hColumnDescriptor);
                }
                admin.createTable(hTableDescriptor);
            }

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
                insertData(TABLE_NAME, user, cols[0]);
            }
            List<User> users = getAllData(TABLE_NAME);
            for (User u : users) {
                System.out.println(u.toString());
            }

            getNoDealData(TABLE_NAME);
            System.out.println("--------------------根据rowKey查询--------------------");
            User user4 = getDataByRowKey(TABLE_NAME, "user-k-u_967");
            System.out.println(user4.toString());

            System.out.println("--------------------获取指定单条数据-------------------");
            String user_phone = getCellData(TABLE_NAME, "user-k-u_953", cols[0], "phone");
            System.out.println(user_phone);
    }


    //连接集群
    public static Connection initHbase() throws IOException {
        // 配置内容参见hbase-site.xml文件.
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "node01.test.bigdata.hbh,node02.test.bigdata.hbh,node03.test.bigdata.hbh");
        configuration.set("zookeeper.znode.parent", "/hbase-unsecure");
        // 集群配置↓
        return ConnectionFactory.createConnection(configuration);
    }

    // 插入数据
    public static void insertData(String tableName, User user, String cfName) throws IOException {
        TableName tablename = TableName.valueOf(tableName);
        Put put = new Put((user.getId()).getBytes());
        //参数：1.列族名  2.列名  3.值
        byte[] bytes = cfName.getBytes();
        put.addColumn(bytes, "username".getBytes(), user.getUsername().getBytes());
        put.addColumn(bytes, "age".getBytes(), Bytes.toBytes(user.getAge()));
        put.addColumn(bytes, "gender".getBytes(), user.getGender().getBytes());
        put.addColumn(bytes, "phone".getBytes(), user.getPhone().getBytes());
        put.addColumn(bytes, "email".getBytes(), user.getEmail().getBytes());
        Table table = HBASE_CONNECTION.getTable(tablename);
        table.put(put);
    }

    //获取原始数据
    public static void getNoDealData(String tableName) {
        try {
            Table table = initHbase().getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                System.out.println("scan:  " + result);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //根据rowKey进行查询
    public static User getDataByRowKey(String tableName, String rowKey) throws IOException {

        Table table = initHbase().getTable(TableName.valueOf(tableName));
        Get get = new Get(rowKey.getBytes());
        User user = new User();
        user.setId(rowKey);
        //先判断是否有此条数据
        if (!get.isCheckExistenceOnly()) {
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                if (colName.equals("username")) {
                    user.setUsername(value);
                } else if (colName.equals("age")) {
                    user.setAge(Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                } else if (colName.equals("gender")) {
                    user.setGender(value);
                } else if (colName.equals("phone")) {
                    user.setPhone(value);
                } else if (colName.equals("email")) {
                    user.setEmail(value);
                }
            }
        }
        return user;
    }

    //查询指定单cell内容
    public static String getCellData(String tableName, String rowKey, String family, String col) {

        try {
            Table table = initHbase().getTable(TableName.valueOf(tableName));
            Get get = new Get(rowKey.getBytes());
            if (!get.isCheckExistenceOnly()) {
                get.addColumn(Bytes.toBytes(family), Bytes.toBytes(col));
                Result res = table.get(get);
                byte[] resByte = res.getValue(Bytes.toBytes(family), Bytes.toBytes(col));
                return Bytes.toString(resByte);
            } else {
                return "查询结果不存在";
            }
        } catch (IOException e) {
            e.printStackTrace();
            return "出现异常";
        }
    }

    /**
     * 获取指定表的全部数据
     * @param tableName     表名
     * @return              返回用户列表
     */
    public static List<User> getAllData(String tableName) {

        Table table;
        List<User> list = new ArrayList<User>();
        try {
            table = initHbase().getTable(TableName.valueOf(tableName));
            ResultScanner results = table.getScanner(new Scan());
            User user = null;
            for (Result result : results) {
                String id = new String(result.getRow());
                System.out.println("用户名:" + new String(result.getRow()));
                user = new User();
                for (Cell cell : result.rawCells()) {
                    String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    //String family =  Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength());
                    String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    user.setId(row);
                    if (colName.equals("username")) {
                        user.setUsername(value);
                    } else if (colName.equals("age")) {
                        user.setAge(Bytes.toInt(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    } else if (colName.equals("gender")) {
                        user.setGender(value);
                    } else if (colName.equals("phone")) {
                        user.setPhone(value);
                    } else if (colName.equals("email")) {
                        user.setEmail(value);
                    }
                }
                list.add(user);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return list;
    }


    /**
     * 删除指定cell数据
     *
     * @param tableName     表名
     * @param rowKey        rowKey
     * @throws IOException  IO异常
     */
    public static void deleteByRowKey(String tableName, String rowKey) throws IOException {

        Table table = initHbase().getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        //删除指定列
        //delete.addColumns(Bytes.toBytes("contact"), Bytes.toBytes("email"));
        table.delete(delete);
    }


    /**
     * 删除指定表
     * @param tableName 表名
     */
    public static void deleteTable(String tableName) {

        try {
            TableName tablename = TableName.valueOf(tableName);
            admin = initHbase().getAdmin();
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}