package core.cache;

/**
 * cache 传输的数据
 */
public class JsonMessage {
    private int code;
    private String data;

    public JsonMessage(int code, String data) {
        this.code = code;
        this.data = data;
    }

    /**
     * 根据这个对象的toString 生成的字符串，构造对象
     * @param str
     */
    public static JsonMessage getJsonMessage(String str){
        String temp[] = str.split("\"");
        String data = temp[5];
        String cTemp = temp[2];
        int code = Integer.valueOf(cTemp.substring(1,cTemp.length()-1));
        return  new JsonMessage(code,data);
    }
    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;

    }

    public String getData() {
        return data;
    }
    public void setData(String data) {
        this.data = data;
    }
    @Override
    public String toString() {
        return "{\"code\":" +code+","+
                "\"data\":" +"\""+data+"\""+"}";
        /**
         * {"code":200,"data": "val"}
         */

    }
}
