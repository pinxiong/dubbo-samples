/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.samples.extensibility.registry.common;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.registry.NotifyListener;
import org.apache.dubbo.registry.support.FailbackRegistry;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.dubbo.common.constants.RegistryConstants.CATEGORY_KEY;
import static org.apache.dubbo.common.constants.RegistryConstants.DEFAULT_CATEGORY;

public class FileRegistry extends FailbackRegistry {
    protected FileRegistry(URL url) {
        super(url);
        this.filePath = new File(url.getUrlParam().getParameter(Constants.FILE_PATH));
        this.monitor = new FileMonitorImpl(this.filePath.getAbsolutePath(), SUFFIX);
        this.monitor.start();
    }

    private static final Logger logger = LoggerFactory.getLogger(FileRegistry.class);
    private static final String SUFFIX = ".properties";

    private final FileMonitor monitor;

    private final File filePath;

    private final Map<String, FileListener> fileListenerMap = new ConcurrentHashMap<>();

    private String getServicePath(URL url) {
        return this.filePath.getAbsolutePath() + File.separator + url.getServiceInterface();
    }

    private String getCategoryPath(URL url) {
        String servicePath = getServicePath(url);
        File directory = new File(servicePath);
        if (!directory.exists()) {
            directory.mkdirs();
        }
        return servicePath + File.separator + url.getParameter(CATEGORY_KEY, DEFAULT_CATEGORY);
    }

    @Override
    public void doRegister(URL url) {
        String fileName = this.getCategoryPath(url);
        String content = url.toFullString();
        File newFile = new File(fileName);
        if (!newFile.exists()) {
            try {
                if (newFile.createNewFile()) {
                    logger.info("The file is created. File: " + newFile.getAbsolutePath());
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        try {
            FileWriter out = new FileWriter(newFile, true);
            out.write(content);
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void doUnregister(URL url) {
        String fileName = this.getCategoryPath(url);
        File newFile = new File(fileName);
        if (newFile.exists()) {
            if (newFile.delete() && newFile.getParentFile().delete()) {
                logger.info("The file is deleted. File: " + newFile.getAbsolutePath());
            }
        }
    }

    @Override
    public void doSubscribe(URL url, final NotifyListener listener) {
        final String fileName = this.getServicePath(url);
        this.fileListenerMap.putIfAbsent(fileName, file -> {
            if (file.getName().equals(fileName)) {
                listener.notify();
            }
        });
        this.monitor.addFileListener(this.fileListenerMap.get(fileName));
    }

    @Override
    public void doUnsubscribe(URL url, NotifyListener listener) {
        final String fileName = this.getServicePath(url);
        this.monitor.removeFileListener(this.fileListenerMap.remove(fileName));
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public void destroy() {
        super.destroy();
        this.monitor.stop();
    }
}
