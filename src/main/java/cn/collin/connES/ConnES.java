package cn.collin.connES;

import cn.collin.vertxWeb.VertxWeb;
import io.vertx.core.Vertx;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;

/**
 * Created by collin on 17-5-19.
 */
public class ConnES {
    /*private JSONObject chartData;
    private JSONObject labels;
    private int[] labelFreq;*/
    private JSONObject barData = new JSONObject();
    private String scroll_id = "";
    private JSONArray array = new JSONArray();
    private long start;
    private long end;
    private int[] amount;
    private String[] labels;
    private String scrollUrl = "http://localhost:9200/_search/scroll";
    private String dataUrl = "http://localhost:9200/chain/code/_search?scroll=1m";
    private JSONArray temp = new JSONArray();
    private SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss:SSS");


    public JSONObject searchData (String serverId, Long startTime, Long endTime) throws IOException {
        array.clear();
        barData.clear();
        amount = new int[6];
        labels = new String[6];
        scroll_id = "";
        String jsonData = getJsonData(serverId, startTime, endTime);

        String rpData = sendPost(dataUrl, jsonData);
        handleResponse(rpData);
        if (array.isEmpty()){
            System.out.println("no data");
            barData.put("data", "null");
            barData.put("labels", "null");
        } else {
            System.out.println("success");
            start = getStartTime(array.getJSONObject(array.size() - 1));
            end = getStartTime(array.getJSONObject(0));
            System.out.println("start:" + start);
            System.out.println("end:" + end);
            double interval = Math.rint((end - start) / 5);

            /******************************************/
            long t1 = (long) (start + interval);
            long t2 = (long) (start + 2 * interval);
            long t3 = (long) (start + 3 * interval);
            long t4 = (long) (start + 4 * interval);

            labels[0] = formatter.format(start);
            labels[1] = formatter.format(t1);
            labels[2] = formatter.format(t2);
            labels[3] = formatter.format(t3);
            labels[4] = formatter.format(t4);
            labels[5] = formatter.format(end);

            for (int i = 0; i < array.size(); i++) {
                long iStart = getStartTime(array.getJSONObject(i));
                if (iStart < t1) {
                    amount[1]++;
                } else if (iStart < t2) {
                    amount[2]++;
                } else if (iStart < t3) {
                    amount[3]++;
                } else if (iStart < t4) {
                    amount[4]++;
                } else {
                    amount[5]++;
                }
            }
            barData.put("data", JSONArray.fromObject(amount));
            barData.put("labels", JSONArray.fromObject(labels));

            System.out.println(barData.toString());
        }
        return barData;

    }

    public void handleResponse (String response) {
        JSONObject result = JSONObject.fromObject(response);
        JSONObject hits = result.getJSONObject("hits");
        temp = hits.getJSONArray("hits");
        scroll_id = result.getString("_scroll_id");
        System.out.println("_scroll_id:"+scroll_id);
        while (!temp.isEmpty()){
            array.addAll(temp);
            String jsonData = "{" +
                    "\"scroll\" : \"1m\"," +
                    "\"scroll_id\" : \""+ scroll_id +"\"" +
                    "}";
            try {
                String scrollRp = sendPost(scrollUrl, jsonData);
                temp.clear();
                handleResponse(scrollRp);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }



    }


    public long getStartTime (JSONObject jsonObject){
        return jsonObject.getJSONObject("_source").getLong("startTime");
    }

    public String getJsonData (String serverId, Long startTime, Long endTime) {
        String jsonData = "{" +
                "\"size\": 100," +
                "\"sort\":[{\"startTime\":\"desc\"}],"+
                "\"query\": {" +
                "\"bool\": {" +
                "\"must\": [" +
                "{\"match\": {\"serverId\":\"" + serverId    +"\"}}" +
//                "{\"match\": {\"serverId\":\"http://202.120.167.86:7050/chaincode\"}}" +
                "]," +
                "\"filter\": [" +
                "{\"range\": {\"startTime\": {\"gte\":" + startTime + ",\"lte\":" + endTime +" }}}" +
                "]" +
                "}" +
                "}" +
                "}";
        System.out.println(jsonData);
        return jsonData;
    }

    public String sendPost(String url,String Params)throws IOException {
        OutputStreamWriter out = null;
        BufferedReader reader = null;
        String response="";
        try {
            URL httpUrl = null; //HTTP URL类 用这个类来创建连接
            //创建URL
            httpUrl = new URL(url);
            //建立连接
            HttpURLConnection conn = (HttpURLConnection) httpUrl.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("connection", "keep-alive");
            conn.setUseCaches(false);//设置不要缓存
            conn.setInstanceFollowRedirects(true);
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.connect();
            //POST请求
            out = new OutputStreamWriter(
                    conn.getOutputStream());
            out.write(Params);
            out.flush();
            //读取响应
            reader = new BufferedReader(new InputStreamReader(
                    conn.getInputStream()));
            String lines;
            while ((lines = reader.readLine()) != null) {
                lines = new String(lines.getBytes(), "utf-8");
                response+=lines;
            }
            reader.close();
            // 断开连接
            conn.disconnect();
        } catch (Exception e) {
            System.out.println("发送 POST 请求出现异常！"+e);
            e.printStackTrace();
        }
        //使用finally块来关闭输出流、输入流
        finally{
            try{
                if(out!=null){
                    out.close();
                }
                if(reader!=null){
                    reader.close();
                }
            }
            catch(IOException ex){
                ex.printStackTrace();
            }
        }

        return response;
    }


}
