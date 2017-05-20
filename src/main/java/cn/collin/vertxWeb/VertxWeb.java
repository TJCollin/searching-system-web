package cn.collin.vertxWeb;

import cn.collin.connES.ConnES;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import net.sf.json.JSONObject;

import java.io.IOException;

/**
 * Created by collin on 17-5-14.
 */
public class VertxWeb extends AbstractVerticle{
    public static Vertx vertx;
    private String serverId = "";
    private Long searchStart;
    private Long searchEnd;
    private ConnES connES = new ConnES();
//    private JSONObject barData = new JSONObject();

    public static void main(String[] args) {
        vertx = Vertx.vertx();
        // 部署发布rest服务
        vertx.deployVerticle(new VertxWeb());
//        VertxOptions
    }

    @Override
    public void start() throws Exception {
        final Router router = Router.router(vertx);
        router.route().handler(CorsHandler.create("*")
                .allowedMethod(HttpMethod.GET)
                .allowedMethod(HttpMethod.POST)
                .allowedMethod(HttpMethod.OPTIONS)
                .allowedHeader("X-PINGARUNER")
                .allowedHeader("Content-Type"));
        router.route().handler(BodyHandler.create());
        router.post("/getData").handler(this::getData);
        /*router.post("/endInvoke").handler(this::endInvoke);
        router.post("/getRealtimeData").handler(this::getRealtimeData);
        router.get("/endTest").handler(this::endTest);*/
        vertx.createHttpServer().requestHandler(router::accept).listen(1112);
    }

    private void getData(RoutingContext context) {

        serverId = context.getBodyAsJson().getString("testUrl");
//        System.out.println(serverId);
        searchStart = context.getBodyAsJson().getLong("startTime");
//        System.out.println(searchStart);
        searchEnd = context.getBodyAsJson().getLong("endTime");
        try {
            JSONObject barData = connES.searchData(serverId, searchStart, searchEnd);
            System.out.println("barData:"+barData.toString());
            context.response().end(barData.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
       /* try {
            boolean finish = connES.searchData(serverId, searchStart, searchEnd);
            if (finish){

            } else {

            }
        } catch (IOException e) {
            e.printStackTrace();
        }*/
//        System.out.println(searchEnd);
//        barData = connES.searchData(serverId, searchStart, searchEnd);
//        System.out.println(barData.toString());
//        if (!barData.isEmpty()){
//            context.response().end(barData.toString());
//        }

//        context.response().end("{\"result\":\"ok\"}");
    }
}
