package com.gkgd.bigscreen.util;

import com.alibaba.fastjson.JSONArray;

/**
 * @ModelName
 * @Description
 * @Author zhangjinhang
 * @Date 2020/11/20 15:09
 * @Version V1.0.0
 */
public class AreaUtil {
    public static boolean inArea(double pointLon, double pointLat, String geos) {
        // 代表有几个点
        String[] ges = geos.split(";");
        int vertexNum = ges.length;
        boolean result = false;
        for (int i = 0, j = vertexNum - 1; i < vertexNum; j = i++) {
            // 满足条件，与多边形相交一次，result布尔值取反一次，奇数个则在区域内

            Double lon_i = Double.valueOf(ges[i].split(",")[0]);
            Double lat_i = Double.valueOf(ges[i].split(",")[1]);

            Double lon_j = Double.valueOf(ges[j].split(",")[0]);
            Double lat_j = Double.valueOf(ges[j].split(",")[1]);

            if ((lon_i > pointLon) != (lon_j > pointLon)
                    && (pointLat < (lat_j - lat_i) * (pointLon - lon_i) / (lon_j - lon_i)
                    + lat_i)) {
                result = !result;
            }
        }
        return result;
    }
    public static boolean inArea2(double pointLon, double pointLat, String geos) {
        JSONArray jsonArray = JSONArray.parseArray(geos);
        int vertexNum = jsonArray.size();
        boolean result = false;
        for (int i = 0, j = vertexNum - 1; i < vertexNum; j = i++) {
            // 满足条件，与多边形相交一次，result布尔值取反一次，奇数个则在区域内
            JSONArray li = (JSONArray)jsonArray.get(i);
            Double lon_i = li.getDouble(0);
            Double lat_i = li.getDouble(1);

            JSONArray lj = (JSONArray)jsonArray.get(j);
            Double lon_j = lj.getDouble(0);
            Double lat_j = lj.getDouble(1);

            if ((lon_i > pointLon) != (lon_j > pointLon)
                    && (pointLat < (lat_j - lat_i) * (pointLon - lon_i) / (lon_j - lon_i)
                    + lat_i)) {
                result = !result;
            }
        }
        return result;
    }
}

