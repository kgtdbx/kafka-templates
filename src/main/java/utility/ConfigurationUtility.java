package utility;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;

public class ConfigurationUtility {

    public static Configuration getConfiguration(String configPath) {

        Configuration config = null;
        Configurations configs = new Configurations();

        try {
            config = configs.properties(new File(configPath));
        }
        catch (ConfigurationException ex) {
            ex.printStackTrace();
        }

        return config;
    }
}