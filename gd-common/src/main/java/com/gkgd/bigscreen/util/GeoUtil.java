package com.gkgd.bigscreen.util;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.gavaghan.geodesy.Ellipsoid;
import org.gavaghan.geodesy.GeodeticCalculator;
import org.gavaghan.geodesy.GeodeticCurve;
import org.gavaghan.geodesy.GlobalCoordinates;

import java.io.IOException;

import com.alibaba.fastjson.JSON;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;

import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class GeoUtil {

    public static CloseableHttpClient httpclient = HttpClients.createDefault();
    public static String queryUrl = "https://api.map.baidu.com/geocoder/v2/?";


    static double pi = 3.14159265358979324;
    static double a = 6378245.0;
    static double ee = 0.00669342162296594323;
    public final static double x_pi = 3.14159265358979324 * 3000.0 / 180.0;


    public static double[] wgs2bd(double lat, double lon) {
        double[] wgs2gcj = wgs2gcj(lat, lon);
        double[] gcj2bd = gcj2bd(wgs2gcj[0], wgs2gcj[1]);
        return gcj2bd;
    }

    public static double[] gcj2bd(double lat, double lon) {
        double x = lon, y = lat;
        double z = Math.sqrt(x * x + y * y) + 0.00002 * Math.sin(y * x_pi);
        double theta = Math.atan2(y, x) + 0.000003 * Math.cos(x * x_pi);
        double bd_lon = z * Math.cos(theta) + 0.0065;
        double bd_lat = z * Math.sin(theta) + 0.006;
        return new double[] { bd_lat, bd_lon };
    }

    public static double[] bd2gcj(double lat, double lon) {
        double x = lon - 0.0065, y = lat - 0.006;
        double z = Math.sqrt(x * x + y * y) - 0.00002 * Math.sin(y * x_pi);
        double theta = Math.atan2(y, x) - 0.000003 * Math.cos(x * x_pi);
        double gg_lon = z * Math.cos(theta);
        double gg_lat = z * Math.sin(theta);
        return new double[] { gg_lat, gg_lon };
    }

    public static double[] wgs2gcj(double lat, double lon) {
        double dLat = transformLat(lon - 105.0, lat - 35.0);
        double dLon = transformLon(lon - 105.0, lat - 35.0);
        double radLat = lat / 180.0 * pi;
        double magic = Math.sin(radLat);
        magic = 1 - ee * magic * magic;
        double sqrtMagic = Math.sqrt(magic);
        dLat = (dLat * 180.0) / ((a * (1 - ee)) / (magic * sqrtMagic) * pi);
        dLon = (dLon * 180.0) / (a / sqrtMagic * Math.cos(radLat) * pi);
        double mgLat = lat + dLat;
        double mgLon = lon + dLon;
        double[] loc = { mgLat, mgLon };
        return loc;
    }

    private static double transformLat(double lat, double lon) {
        double ret = -100.0 + 2.0 * lat + 3.0 * lon + 0.2 * lon * lon + 0.1 * lat * lon + 0.2 * Math.sqrt(Math.abs(lat));
        ret += (20.0 * Math.sin(6.0 * lat * pi) + 20.0 * Math.sin(2.0 * lat * pi)) * 2.0 / 3.0;
        ret += (20.0 * Math.sin(lon * pi) + 40.0 * Math.sin(lon / 3.0 * pi)) * 2.0 / 3.0;
        ret += (160.0 * Math.sin(lon / 12.0 * pi) + 320 * Math.sin(lon * pi  / 30.0)) * 2.0 / 3.0;
        return ret;
    }

    private static double transformLon(double lat, double lon) {
        double ret = 300.0 + lat + 2.0 * lon + 0.1 * lat * lat + 0.1 * lat * lon + 0.1 * Math.sqrt(Math.abs(lat));
        ret += (20.0 * Math.sin(6.0 * lat * pi) + 20.0 * Math.sin(2.0 * lat * pi)) * 2.0 / 3.0;
        ret += (20.0 * Math.sin(lat * pi) + 40.0 * Math.sin(lat / 3.0 * pi)) * 2.0 / 3.0;
        ret += (150.0 * Math.sin(lat / 12.0 * pi) + 300.0 * Math.sin(lat / 30.0 * pi)) * 2.0 / 3.0;
        return ret;
    }

    public static double getDistanceMeter(Double lng_from,Double lat_from,Double lng_to,Double lat_to, Ellipsoid ellipsoid){

        GlobalCoordinates source = new GlobalCoordinates(lat_from, lng_from);
        GlobalCoordinates target = new GlobalCoordinates(lat_to, lng_to);
        //计算结果单位米
        GeodeticCurve geoCurve = new GeodeticCalculator().calculateGeodeticCurve(ellipsoid, source, target);

        return geoCurve.getEllipsoidalDistance();
    }

    public static Boolean outLine(Double lng, Double lat, String lineStr){
        String[] lines = lineStr.split("\\|");
        Boolean outLine = true;

        LABLE_START:
        for (String line : lines) {
            JSONArray jsonArray = JSONArray.parseArray(line);
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONArray ll = (JSONArray)jsonArray.get(i);
                for (int j = 0; j < ll.size(); j++) {
                    JSONArray li = (JSONArray)ll.get(j);
                    Double endLng = li.getBigDecimal(0).doubleValue();
                    Double endLat = li.getBigDecimal(1).doubleValue();
                    double distanceMeter = GeoUtil.getDistanceMeter(lng, lat, endLng, endLat, Ellipsoid.Sphere);
                    if(distanceMeter<=500){
                        outLine = false;
                        break LABLE_START;
                    }
                }
            }
        }
        return outLine;
    }


    public static String getAddress(Double lat,Double lng) throws Exception{
        List<NameValuePair> pairs = new ArrayList<>();
        String location = lat+","+lng;
        pairs.add(new BasicNameValuePair("location", location));
        pairs.add(new BasicNameValuePair("output", "json"));
        pairs.add(new BasicNameValuePair("pois", "1"));
        pairs.add(new BasicNameValuePair("latest_admin", "1"));
        pairs.add(new BasicNameValuePair("ak", "pYaA0tXDmyW4i5LpuwNVS60V4gdNkX2S"));
        String url = queryUrl + EntityUtils.toString(new UrlEncodedFormEntity(pairs, "UTF-8"));
        HttpGet httpGet = new HttpGet(url);
        httpGet.addHeader("Content-Type", "application/json");
        CloseableHttpResponse httpResponse = httpclient.execute(httpGet);
        HttpEntity resEntity = httpResponse.getEntity();
        if (resEntity != null) {
            String respStr = EntityUtils.toString(resEntity);
            JSONObject jsonObject = JSON.parseObject(respStr);
            String addr = null;
            if(jsonObject.getJSONObject("result")!=null)
                addr = jsonObject.getJSONObject("result").getString("formatted_address");
            return addr;
        } else {
            return null;
        }
    }
}