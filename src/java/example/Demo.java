import constant.CacheOptions;
import core.client.CacheClient;

public class Demo {
    public static void main(String[] args) {
        CacheClient cacheClient = new CacheClient.newBuilder()
                .setNumberOfReplicas(CacheOptions.numberOfReplicas)
                .setNewNode("1:127.0.0.1:8081:9091")
                .setNewNode("2:127.0.0.1:8082:9092")
                .setNewNode("3:127.0.0.1:8083:9093")
                //.setNewNode("4:127.0.0.1:8084:9094")
                //.setNewNode("5:127.0.0.1:8085:9095")
                .build();
        boolean flag  = cacheClient.set("name", "quan",-1);
        if (flag) {
            String val = cacheClient.get("name");
            System.out.println(val);
        }
        flag = cacheClient.set("sex", "man", 1234506);
        System.out.println(flag);
        System.out.println("name=" + cacheClient.get("name") + " sex = " + cacheClient.get("sex"));

        flag = cacheClient.del("sex");
        System.out.println(flag);
        flag = cacheClient.del("name");
        System.out.println(flag);

        System.out.println("name=" + cacheClient.get("name") + " sex = " + cacheClient.get("sex"));

        System.out.println(cacheClient.status(null));

        cacheClient.close();
    }
}
