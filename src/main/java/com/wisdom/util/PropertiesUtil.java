package com.wisdom.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by BaldKiller
 * on 2019/7/30 22:40
 */
public class PropertiesUtil {
    public static Properties getProperties(String name){
        InputStream resourceAsStream=PropertiesUtil.class.getClassLoader().getResourceAsStream(name+".properties");
        Properties props=new Properties();
        try {
            props.load(resourceAsStream);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            try {
                resourceAsStream.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return props;

    }
}
