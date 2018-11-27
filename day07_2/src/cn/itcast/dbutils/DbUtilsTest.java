package cn.itcast.dbutils;

import cn.itcast.Account;
import cn.itcast.utils.JDBCUtils;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.*;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DbUtilsTest {

    /**
     * 执行添加
     * @throws SQLException
     */
    @Test
    public void test1() throws SQLException {
        QueryRunner queryRunner = new QueryRunner(JDBCUtils.getDataSource());
        int updateCount = queryRunner.update("insert into account values (null, ?, ?)",
                "赵六", 10000);
        System.out.println(updateCount);
    }

    @Test
    public void test2() throws SQLException {
        QueryRunner queryRunner = new QueryRunner(JDBCUtils.getDataSource());
        Account account = queryRunner.query("select * from account where id = ?",
                new ResultSetHandler<Account>() {
                    @Override
                    public Account handle(ResultSet resultSet) throws SQLException {
                        if(resultSet.next()){//1.7版本需要我们自己来判断是否有下一条数据，然后进行获取
                            Account account = new Account();
                            account.setId(resultSet.getInt("id"));
                            account.setName(resultSet.getString("name"));
                            account.setMoney(resultSet.getDouble("money"));
                            return account;
                        }
                        return null;
                    }
                }, 7);
        System.out.println(account);
    }

    /**
     * 演示ResultSetHandler的实现类：ArrayHandler
     *      :将一行记录封装成一个Object[]
     */
    @Test
    public void test3() throws SQLException {
        QueryRunner queryRunner = new QueryRunner(JDBCUtils.getDataSource());
        Object[] objects = queryRunner.query("select * from account where id = ?",
                new ArrayHandler(), 7);
        System.out.println(Arrays.toString(objects));
    }

    /**
     * 演示ResultSetHandler的实现类：ArrayListHandler
     *      :查询多条记录，并且将这些记录一行一行封装成一个一个Object[],然后将这些Object[]存储到List集合中
     *      ：这个JavaBean对象由new BeanListHandler<Account>(Account.class)的参数决定
     * @throws SQLException
     */
    @Test
    public void test4() throws SQLException {
        QueryRunner queryRunner = new QueryRunner(JDBCUtils.getDataSource());
        List<Object[]> list = queryRunner.query("select * from account ",
                new ArrayListHandler());
        for (Object[] objects : list) {
            System.out.println(Arrays.toString(objects));
        }
    }

    /**
     * 演示ResultSetHandler的实现类：BeanListHandler
     *      :查询多条记录，并且将这些记录一行一行封装成一个一个JavaBean,然后将这些JavaBean存储到List集合中
     *
     * @throws SQLException
     */
    @Test
    public void test5() throws SQLException {
        QueryRunner queryRunner = new QueryRunner(JDBCUtils.getDataSource());
        List<Account> list = queryRunner.query("select * from account ",
                new BeanListHandler<Account>(Account.class));
        for (Account account : list) {
            System.out.println(account);
        }
    }

    /**
     * 演示ResultSetHandler的实现类：BeanHandler
     *      :将一条记录封装成一个JavaBean对象
     * @throws SQLException
     */
    @Test
    public void test6() throws SQLException {
        QueryRunner queryRunner = new QueryRunner(JDBCUtils.getDataSource());
        Account account = queryRunner.query("select * from account where id = ?",
                new BeanHandler<Account>(Account.class), 7);
        System.out.println(account);
    }

    /**
     * 演示ResultSetHandler的实现类：MapHandler
     *      :将一条记录封装成一个Map对象，map的key是列名，value是列的值
     */
    @Test
    public void test7() throws SQLException {
        QueryRunner queryRunner = new QueryRunner(JDBCUtils.getDataSource());
        Map<String, Object> map = queryRunner.query("select * from account where id = ?",
                new MapHandler(), 7);
        System.out.println(map);
    }

    /**
     * 演示ResultSetHandler的实现类：MapListHandler
     *      :查询多条记录，将每条记录封装成一个Map，将这些Map封装在一个list中
     */
    @Test
    public void test8() throws SQLException {
        QueryRunner queryRunner = new QueryRunner(JDBCUtils.getDataSource());
        List<Map<String, Object>> list = queryRunner.query("select * from account  ",
                new MapListHandler() );
        System.out.println(list);
    }

    /**
     * 演示ResultSetHandler的实现类：ScalarHandler
     *      :查询的结果是一行一列，封装到一个对象中，常用在聚合函数的查询中：查询记录数
     */
    @Test
    public void test9() throws SQLException {
        QueryRunner queryRunner = new QueryRunner(JDBCUtils.getDataSource());
        Long aLong = queryRunner.query("select count(*) from account ", new ScalarHandler<Long>());
        System.out.println(aLong);
    }

    @Test
    public void test10() throws SQLException {
        QueryRunner queryRunner = new QueryRunner(JDBCUtils.getDataSource());
        String aLong = queryRunner.query("select name from account where id = 7 ", new ScalarHandler<String>());
        System.out.println(aLong);
    }

    /**
     * 演示ResultSetHandler的实现类：ColumnListHandler
     *      查询一列数据，并且将这列数据封装到一个List集合中
     */
    @Test
    public void test11() throws SQLException {
        QueryRunner queryRunner = new QueryRunner(JDBCUtils.getDataSource());
        List<Object> list = queryRunner.query("select * from account", new ColumnListHandler<>("name"));
        System.out.println(list);
    }

    /**
     * 演示ResultSetHandler的实现类：KeyedHandler
     *      查询多条数据，将每一条数据封装成一个Map，再将这些map封装到另一个map中，那么再这个map对象中以指定的名称为键，以行记录map为value
     */
    @Test
    public void test12() throws SQLException {
        QueryRunner queryRunner = new QueryRunner(JDBCUtils.getDataSource());
        Map<Object, Map<String, Object>> map = queryRunner.query("select * from account",
                new KeyedHandler<>("name"));
        System.out.println(map);
    }

    @Test
    public void test13() throws SQLException {
        QueryRunner queryRunner = new QueryRunner(JDBCUtils.getDataSource());
        Map<Object, Map<String, Object>> map = queryRunner.query("select * from account",
                new KeyedHandler<>("id"));
        System.out.println(map);
    }

    /**
     * DbUtils完成事务管理
     *      :创建QueryRunner的时候，不需要连接池参数
     *      ：update方法中必须要传递Connection对象
     */
    @Test
    public void test14(){
        QueryRunner queryRunner = new QueryRunner();
        Connection conn = null;
        try {
            conn = JDBCUtils.getConnection();
            conn.setAutoCommit(false);//开启事务
            //完成转账：给一个人减钱，给另一个人加钱

            //操作1
            queryRunner.update(conn,"update account set money = money + ? where name = ?", -1000.0, "张三");

            int i = 1/0;

            //操作2
            queryRunner.update(conn,"update account set money = money + ? where name = ?", 1000.0, "李四");

            //提交事务
            DbUtils.commitAndCloseQuietly(conn);
        } catch (SQLException e) {
            DbUtils.rollbackAndCloseQuietly(conn);
            e.printStackTrace();
        }
    }
}
