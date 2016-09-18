package com.houbank.encoder;

import java.net.Inet4Address;
import java.net.UnknownHostException;

import net.logstash.logback.encoder.LogstashEncoder;
/**
 * 
 * @author WangBing
 */
public class MyLogstashEncoder extends LogstashEncoder {
    public MyLogstashEncoder() {
        String customFields;
        try {
            customFields = "{\"ip\":\"" + Inet4Address.getLocalHost().getHostAddress() + "\"}";
            this.setCustomFields(customFields);
        } catch (UnknownHostException e) {
        }
    }
    @Override
    public void setCustomFields(String customFields) {
        // TODO Auto-generated method stub
        super.setCustomFields(customFields);
    }

}
