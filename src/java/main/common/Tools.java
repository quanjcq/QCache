package common;

import java.util.ArrayList;
import java.util.List;

public class Tools {
    /**
     * 按照空格分割字符串，对于多个空格当做一个空格，“” 里面的字符不拆开，
     * 若格式不正确返回null;
     * @return
     */
    public static List<String> split(String line) {
        StringBuilder builder = new StringBuilder();
        List<String> res = new ArrayList<String>();
        int num = 0;
        for (int i = 0; i < line.length(); i++) {
            if (num % 2 == 1) { //出现单数双引号
                if (line.charAt(i) == '"') {
                    if (builder.length() > 0) {
                        res.add(builder.toString());
                        builder = new StringBuilder();
                    } else {
                        continue;
                    }
                    num++;
                } else {
                    builder.append(line.charAt(i));
                }
            } else if (num % 2 == 0) {
                if (line.charAt(i) == ' ') {
                    //出现空格
                    if (builder.length() > 0) {
                        res.add(builder.toString());
                        builder = new StringBuilder();
                    } else {
                        continue;
                    }
                } else if (line.charAt(i) == '"') {
                    num++;
                    if (builder.length() > 0) {
                        res.add(builder.toString());
                        builder = new StringBuilder();
                    } else {
                        continue;
                    }
                } else {
                    builder.append(line.charAt(i));
                }
            }
        }
        if (builder.length() > 0) {
            res.add(builder.toString());
        }
        if (num % 2 == 1 || res.size() == 0) {
            return null;
        } else {
            return res;
        }
    }
}
