package com.harry.weather.ui;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.Scanner;

import static java.lang.System.exit;

public class Test {
    public static void main(String[] args) {
        //从用户读取想查询的城市
        Scanner in = new Scanner(System.in);
        System.out.println("请输入想查询的城市(上海 or 北京 or 福州)：");
        String city = in.next();
        String cityIdentity = null;

        //从城市名字获取cityId
        if (city.equals("上海")) {
            cityIdentity = "101020100";
        } else if (city.equals("北京")) {
            cityIdentity = "101010100";
        } else if (city.equals("福州")) {
            cityIdentity = "101230101";
        } else {
            System.out.println("您输入有误，程序终止");
            exit(0);
        }

        try {
            //1、构建一个客户端
            CloseableHttpClient client = HttpClients.createDefault();
            //2、构建请求对象
            String uri1 = "https://geoapi.qweather.com/v2/city/lookup?key=de943f78e86e4f459a2e414f9c0f153c&location=" + city;
            HttpGet doGetCity = new HttpGet(uri1);
            String uri2 = "https://devapi.qweather.com/v7/weather/3d?key=de943f78e86e4f459a2e414f9c0f153c&location=" + cityIdentity;
            HttpGet doGetWeather = new HttpGet(uri2);

            //3、发送请求，服务器响应json字符串
            CloseableHttpResponse CityResponse = client.execute(doGetCity);
            CloseableHttpResponse WeatherResponse = client.execute(doGetWeather);

            //获取响应中的实体
            String jsonStrCity = EntityUtils.toString(CityResponse.getEntity(), "utf-8");
            String jsonStrWeather = EntityUtils.toString(WeatherResponse.getEntity(), "utf-8");

            //4、解析 jsonStr
            ObjectMapper MapperCity = new ObjectMapper();
            CityInfo cities = MapperCity.readValue(jsonStrCity, CityInfo.class);

            ObjectMapper MapperWeather = new ObjectMapper();
            WeatherInfo weathers = MapperWeather.readValue(jsonStrWeather, WeatherInfo.class);
            //5、解析对象的信息
            List<CityInfo.Location> location = cities.getLocation();
            List<WeatherInfo.Daily> daily = weathers.getDaily();

            //6、调用jdbc方法，实现对数据库的增删改查
            jdbc(location, daily);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //tableExists方法判断是否存在表
    public static boolean tableExists(Connection conn, String table) {
        boolean flag = true;
        ResultSet rsTables = null;
        try {
            DatabaseMetaData meta = conn.getMetaData();
            rsTables = meta.getTables(null, null, table, null);
            if (rsTables.next() == false) {
                flag = false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return flag;
    }


    //jdbc方法，实现对数据库的增删改查
    public static void jdbc(List<CityInfo.Location> location, List<WeatherInfo.Daily> daily) {
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;

        //获取连接数据库的url、user、password
        Scanner in = new Scanner(System.in);
        System.out.println("请输入url：");
        String url = in.next();
        System.out.println("请输入数据库用户名(user)：");
        String user = in.next();
        System.out.println("请输入数据库密码(password)：");
        String password = in.next();


        try {
            //1、注册数据库驱动
            Class.forName("com.mysql.cj.jdbc.Driver");
            Driver driver = new com.mysql.cj.jdbc.Driver();
            DriverManager.registerDriver(driver);

            //2、连接数据库
//            String url = "jdbc:mysql://127.0.0.1:3306/weatherreportwest2";
//            String user = "root";
//            String password = "gaoyiren011218";
            conn = DriverManager.getConnection(url, user, password);  //连接数据库的对象

            //3、获取数据库操作对象
            stmt = conn.createStatement();

            //4、执行sql
            //建表(城市表(T_cityInfo) 、 天气表(T_tempInfo))
            //判断表是否已经存在
            //若不存在，则建表
            //表1：城市信息表(城市的基本信息，需要：名字(cityName)、id(cityId)、纬度(cityLatitude)、经度(cityLongitude))
            if (!tableExists(conn, "T_cityInfo")) {
                stmt.executeUpdate("CREATE TABLE T_cityInfo" +
                        "(" +
                        "city_Name VARCHAR(32) NOT NULL," +
                        "city_Id VARCHAR(32) NOT NULL," +
                        "city_Latitude VARCHAR(32) NOT NULL," +
                        "city_Longitude VARCHAR(32) NOT NULL," +
                        "PRIMARY KEY(city_Id)" +
                        ")");
            }

            //表2：天气信息表(城市的天气信息，需要：当日日期(fxDate)，当日最高气温(tempMax)、当日最低气温(tempMin)、白天天气情况(textDay)、名字(cityName))
            if (!tableExists(conn, "T_tempInfo")) {
                stmt.executeUpdate("CREATE TABLE T_tempInfo" +
                        "(" +
                        "fxDate VARCHAR(32) NOT NULL," +
                        "tempMax VARCHAR(32) NOT NULL," +
                        "tempMin VARCHAR(32) NOT NULL," +
                        "textDay VARCHAR(32) NOT NULL," +
                        "cityName VARCHAR(32) NOT NULL," +
                        "PRIMARY KEY(fxDate,cityName)" +
                        ")");
            }

            //插入&更新(REPLACE) 城市信息(T_cityInfo)
            stmt.executeUpdate("REPLACE INTO T_cityInfo (city_Name,city_Id,city_Latitude,city_Longitude) VALUES ('" + location.get(0).getName() + "','" + location.get(0).getId() + "','" + location.get(0).getLat() + "','" + location.get(0).getLon() + "')");

            //插入&更新(REPLACE) 天气信息(T_tempInfo)
            for (int i = 0; i < 3; i++) {
                stmt.executeUpdate("REPLACE INTO T_tempInfo (fxDate,tempMax,tempMin,textDay,cityName) VALUES ('" + daily.get(i).getFxDate() + "','" + daily.get(i).getTempMax() + "','" + daily.get(i).getTempMin() + "','" + daily.get(i).getTextDay() + "','" + location.get(0).getName() + "')");
            }


            //5、处理查询结果集
            String sql = "SELECT * FROM T_tempInfo";
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                //判断城市是否匹配 & 判断日期是否为后三天日期
                if (rs.getString(5).equals(location.get(0).getName()) && rs.getString(1).compareTo(daily.get(0).getFxDate()) >= 0) {
                    String fxDate = rs.getString(1);
                    String tempMax = rs.getString(2);
                    String tempMin = rs.getString(3);
                    String textDay = rs.getString(4);
                    String cityName = rs.getString(5);
                    //输出三天的天气预报
                    System.out.println("城市:" + cityName);
                    System.out.println("日期:" + fxDate);
                    System.out.println("最高气温：" + tempMax);
                    System.out.println("最低气温：" + tempMin);
                    System.out.println("天气情况：" + textDay);
                    System.out.println("-------------------");
                }
            }

        } catch (SQLException se) {
            se.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //6、释放资源 顺序为：rs-->stmt-->conn
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}