package com.iqismart.mongodbTransfer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

public class ApplicationStartup implements ApplicationListener<ContextRefreshedEvent> {

    protected Logger log = LoggerFactory.getLogger(ApplicationStartup.class);

    public void onApplicationEvent(ContextRefreshedEvent event){
        RestoreMongoDbTask restoreMongoDbTask = event.getApplicationContext().getBean(RestoreMongoDbTask.class);
        restoreMongoDbTask.start();
    }
}

