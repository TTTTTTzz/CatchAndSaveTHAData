package db;

import bean.AsciiFile;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DBUtil {
    private static Connection conn = null;
    private static PreparedStatement pstmt = null;
    private static ResultSet rs = null;
    private static String driver = "oracle.jdbc.OracleDriver";
    private static String url = "jdbc:oracle:thin:@localhost:1521:orcl";
    private static String user = "C##ora1";
    private static String password = "Ora12345";

    public static void insert(String name, long size, Timestamp downloadTime, Timestamp dataTime) {
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("数据库连接成功" + conn);
            String sql = "insert into ASCIIFILE(ASCII_NAME,ASCII_SIZE,DOWNLOAD_TIME,DATA_TIME) values (?,?,?,?)";

            pstmt = conn.prepareStatement(sql);
            //pstmt.setInt(1, id);
            pstmt.setString(1, name);
            pstmt.setDouble(2, size);
            pstmt.setTimestamp(3, downloadTime);
            pstmt.setTimestamp(4, dataTime);

            int res = pstmt.executeUpdate();//执行sql语句
            if (res > 0) {
                System.out.println("数据录入成功");
            }
            pstmt.close();//关闭资源
            conn.close();//关闭资源
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    //获取最新文件的数据时间
    public static void selectLastest(String output) {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output), "GB2312"))) {

            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("数据库连接成功" + conn);
            String sql = "select * from (select * from ASCIIFILE  order by DATA_TIME desc) where rownum = 1";
            // String sql="select to_char(DATA_TIME,'yyyy-MM-dd') AS 日期,count(*) AS 下载文件数量 from ASCIIFILE GROUP BY to_char(DATA_TIME,'yyyy-MM-dd')";

            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();//执行sql语句
            bufferedWriter.write("最新文件数据时间\n");
            if (rs.next()) {
                bufferedWriter.write("'" + rs.getTimestamp("DATA_TIME"));
                bufferedWriter.flush();
            }

            pstmt.close();//关闭资源
            conn.close();//关闭资源
        } catch (ClassNotFoundException | SQLException | IOException e) {
            e.printStackTrace();
        }
    }

    //按天统计每天下载的文件数量（文件数据时间）
    public static void statisticsByDay(String output) {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output), "GB2312"))) {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("数据库连接成功" + conn);
            String sql = "select to_char(DATA_TIME,'yyyy-MM-dd') AS 日期,count(*) AS 下载文件数量 from ASCIIFILE GROUP BY to_char(DATA_TIME,'yyyy-MM-dd') ORDER BY 日期 DESC";

            pstmt = conn.prepareStatement(sql);

            rs = pstmt.executeQuery();//执行sql语句
            bufferedWriter.write("日期,");
            bufferedWriter.write("下载文件数量\n");
            bufferedWriter.flush();
            while (rs.next()) {
                bufferedWriter.write(rs.getDate("日期").toString() + ",");
                bufferedWriter.write(rs.getInt("下载文件数量") + "\n");
                bufferedWriter.flush();
            }

            pstmt.close();//关闭资源
            conn.close();//关闭资源
        } catch (SQLException | ClassNotFoundException | IOException throwables) {
            throwables.printStackTrace();
        }
    }

    //按小时统计每天下载的文件数量（文件数据时间）
    public static void statisticsByHour(String output) {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output), "GB2312"))) {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("数据库连接成功" + conn);
            String sql = "select to_char(DATA_TIME,'yyyy-MM-dd HH24') AS 时间,count(*) AS 下载文件数量 from ASCIIFILE GROUP BY to_char(DATA_TIME,'yyyy-MM-dd HH24') ORDER BY 时间 DESC";

            pstmt = conn.prepareStatement(sql);

            rs = pstmt.executeQuery();//执行sql语句
            bufferedWriter.write("时间(yyyy-MM-dd HH),");
            bufferedWriter.write("下载文件数量\n");
            bufferedWriter.flush();
            while (rs.next()) {
                bufferedWriter.write(rs.getString("时间") + "时,");
                bufferedWriter.write(rs.getInt("下载文件数量") + "\n");
                bufferedWriter.flush();
            }

            pstmt.close();//关闭资源
            conn.close();//关闭资源
        } catch (SQLException | ClassNotFoundException | IOException throwables) {
            throwables.printStackTrace();
        }
    }

    //按天统计每天下载的文件是否有缺少，如有，列出具体没有数据文件的某几个小时
    public static void statisticsByHourAndCheck(String output) {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(output), "GB2312"))) {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("数据库连接成功" + conn);
            String sql = "SELECT to_char( DATA_TIME, 'yyyy-MM-dd' ) AS 日期, count( * ) AS 下载文件数量," +
                    "    DECODE( count( * ), 24, '否', '是' ) AS 是否缺少文件" +
                    "    FROM ASCIIFILE" +
                    "    GROUP BY" +
                    "    to_char( DATA_TIME, 'yyyy-MM-dd' )" +
                    "    ORDER BY" +
                    "    日期 DESC";

            pstmt = conn.prepareStatement(sql);

            rs = pstmt.executeQuery();//执行sql语句
            bufferedWriter.write("日期(yyyy-MM-dd),");
            bufferedWriter.write("是否缺少文件,");
            bufferedWriter.write("缺少文件的时间段\n")
            ;
            bufferedWriter.flush();
            while (rs.next()) {
                bufferedWriter.write(rs.getString("日期") + ",");
                bufferedWriter.write(rs.getString("是否缺少文件"));
                if (rs.getString("是否缺少文件").equals("是")) {
                    for (int i = 0; i < 24; i++) {
                        AsciiFile asciiFile = selectOne(rs.getString("日期") + " " + i);
                        if (asciiFile == null) {
                            if (i != 0) {
                                bufferedWriter.write(",");
                            }
                            bufferedWriter.write("," + i + "时\n");
                            bufferedWriter.flush();
                        }
                    }
                } else {
                    bufferedWriter.write(",无\n");
                }
                bufferedWriter.flush();
            }

            pstmt.close();//关闭资源
            conn.close();//关闭资源
        } catch (SQLException | ClassNotFoundException | IOException throwables) {
            throwables.printStackTrace();
        }
    }

    //根据输入的年月日小时，输出该小时对应的数据文件的全部信息
    private static AsciiFile selectOne(String input) {
        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("数据库连接成功" + conn);
            String sql = "select * from ASCIIFILE where DATA_TIME =TO_DATE(?, 'yyyy-mm-dd HH24')\t";

            PreparedStatement current_pstmt = conn.prepareStatement(sql);
          //  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");
          //  Date dataTimeTemp = sdf.parse(input);
           // Timestamp dataTime = new Timestamp(dataTimeTemp.getTime());
         //   current_pstmt.setTimestamp(1, dataTime);
            current_pstmt.setString(1, input);

            ResultSet current_rs = current_pstmt.executeQuery();//执行sql语句
            AsciiFile result = null;
            if (current_rs.next()) {
                result = new AsciiFile();
                result.setId(current_rs.getInt("ASCII_ID"));
                result.setName(current_rs.getString("ASCII_NAME"));
                result.setSize(current_rs.getLong("ASCII_SIZE"));
                result.setDownloadTime(current_rs.getTimestamp("DOWNLOAD_TIME"));
                result.setDataTime(current_rs.getTimestamp("DATA_TIME"));
            }

            current_pstmt.close();
            return result;
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    //selectOne的包裹方法
    public static void selectAndPrintOne(String outputPath, String input) {
        try (BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(outputPath), "GB2312"))) {
            AsciiFile asciiFile = selectOne(input);
            if (asciiFile != null) {
                bufferedWriter.write("文件名,文件大小（kb）,下载时间,数据时间\n");
                bufferedWriter.flush();
                bufferedWriter.write(asciiFile.getName() + "," +
                        asciiFile.getSize() + ",'" +
                        asciiFile.getDownloadTime().toString() + ",'" +
                        asciiFile.getDataTime().toString());
                bufferedWriter.flush();
            } else {
                bufferedWriter.write("数据库中查不到对应数据！");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
