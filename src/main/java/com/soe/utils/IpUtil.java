package com.soe.utils;

import java.io.File;
import java.net.InetAddress;
import java.net.URL;

import com.maxmind.db.CHMCache;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.model.CountryResponse;
import com.maxmind.geoip2.record.Country;
import org.junit.Test;
//import com.maxmind.db.CHMCache;

/**
 * IP工具类
 *
 * @author Lenjor
 * @version 1.0
 * @date 2020/12/31 11:21
 */
public class IpUtil {
    @Test
    public  void testGetNationByIP(){
//        String ip = "117.136.12.79";
//        String ip = "209.141.58.148";
        String ip = "110.122.144.115";
        try {
            // 读取当前工程下的IP库文件
            URL countryUrl = IpUtil.class.getClassLoader().getResource("GeoLite2-Country.mmdb");
            File countryFile = new File(countryUrl.getPath());

            // 读取IP库文件
            DatabaseReader countryReader = (new DatabaseReader.Builder(countryFile).withCache(new CHMCache())).build();
            CountryResponse countryResponse = countryReader.country(InetAddress.getByName(ip));
            Country country = countryResponse.getCountry();
            //isoCode
            System.out.println("从country IP库读取国家结果： " + country);
            System.out.println(country.getIsoCode());
//            return country.getIsoCode();
        } catch (Exception e) {
            e.printStackTrace();
        }
//        return null;
    }

    public static String getNationByIP(String ip){
//        String ip = "117.136.12.79";
//        String ip = "209.141.58.148";
        try {
            // 读取当前工程下的IP库文件
            URL countryUrl = IpUtil.class.getClassLoader().getResource("GeoLite2-Country.mmdb");
            File countryFile = new File(countryUrl.getPath());

            // 读取IP库文件
            DatabaseReader countryReader = (new DatabaseReader.Builder(countryFile).withCache(new CHMCache())).build();
            CountryResponse countryResponse = countryReader.country(InetAddress.getByName(ip));
            Country country = countryResponse.getCountry();
        //isoCode
//            System.out.println("从country IP库读取国家结果： " + country);
//            System.out.println(country.getIsoCode());
            return country.getIsoCode();
        } catch (Exception e) {
//            e.printStackTrace();
        }
        return null;
    }

}