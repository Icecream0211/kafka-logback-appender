package com.houbank.kafka;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.houbank.failover.FailedCallback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.spi.AppenderAttachable;
import ch.qos.logback.core.spi.AppenderAttachableImpl;
/**
 * 
 * @author WangBing
 */
public class KafkaLogbackAppender extends KafkaLogbackConfigBase<ILoggingEvent> implements AppenderAttachable<ILoggingEvent>{
    
    
    private final AppenderAttachableImpl<ILoggingEvent> aai = new AppenderAttachableImpl<ILoggingEvent>();
    protected Producer<byte[], byte[]> producer = null;
    private final FailedCallback<ILoggingEvent> failedCallback = new FailedCallback<ILoggingEvent>() {
        @Override
        public void onFailed(ILoggingEvent evt, Throwable throwable) {
            aai.appendLoopOnAppenders(evt);
        }
    };
    public void start() {
        super.start();
        Properties props = getProducerProperties();
        this.producer = createKafkaProducer(props);
        addInfo("Kafka producer connected to " + brokerList);
        addInfo("Logging for topic: " + topic);
    }

    @Override
    public void stop() {
        super.stop();
        if (producer != null) {
            producer.close();
        }
    }

    protected void append(ILoggingEvent event) {
        byte[] message = null;
        if (encoder != null) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                encoder.init(baos);
                encoder.setContext(getContext());
                encoder.doEncode(event);
                message = baos.toByteArray();
            } catch (IOException ex) {
                addError("Error encoding event", ex);
            }
        } else {
            message = event.getMessage().getBytes();
        }
        try{
            Future<RecordMetadata> response = producer.send(new ProducerRecord<byte[], byte[]>(topic, message),new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception!=null){
                        failedCallback.onFailed(event,exception);
                    }
                }
            });
            if (syncSend) {
                response.get();
            }
        }catch(Exception ex){
            addError("Error waiting for Kafka response", ex);
            failedCallback.onFailed(event,ex);
        }
    }

    protected Producer createKafkaProducer(Properties props) {
        return new KafkaProducer<byte[], byte[]>(props);
    }
    //attachement appender
    @Override
    public void addAppender(Appender<ILoggingEvent> newAppender) {
       aai.addAppender(newAppender);
    }

    @Override
    public Iterator<Appender<ILoggingEvent>> iteratorForAppenders() {
        // TODO Auto-generated method stub
        return aai.iteratorForAppenders();
    }

    @Override
    public Appender<ILoggingEvent> getAppender(String name) {
        // TODO Auto-generated method stub
        return aai.getAppender(name);
    }

    @Override
    public boolean isAttached(Appender<ILoggingEvent> appender) {
        // TODO Auto-generated method stub
        return aai.isAttached(appender);
    }

    @Override
    public void detachAndStopAllAppenders() {
        aai.detachAndStopAllAppenders();
        
    }

    @Override
    public boolean detachAppender(Appender<ILoggingEvent> appender) {
        // TODO Auto-generated method stub
        return aai.detachAppender(appender);
    }

    @Override
    public boolean detachAppender(String name) {
        // TODO Auto-generated method stub
        return aai.detachAppender(name);
    }
}
