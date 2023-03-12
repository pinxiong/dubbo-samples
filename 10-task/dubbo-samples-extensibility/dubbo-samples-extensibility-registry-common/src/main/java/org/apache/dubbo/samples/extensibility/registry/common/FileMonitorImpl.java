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

import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.HiddenFileFilter;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ConcurrentHashSet;

import java.io.File;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class FileMonitorImpl implements FileMonitor {

    public FileMonitorImpl(String filePath, String suffix) {
        this.monitor = this.createMonitor(filePath,suffix);
    }
    private static final Logger logger = LoggerFactory.getLogger(FileMonitorImpl.class);

    private static final long INTERVAL = TimeUnit.SECONDS.toMillis(1000);

    private final Set<FileListener> fileListeners = new ConcurrentHashSet<>();

    private final FileAlterationMonitor monitor;
    private FileAlterationMonitor createMonitor(String filePath, String suffix) {
        File file = new File(filePath);
        if (!file.exists()) {
            file.mkdirs();
        }
        // create filter
        IOFileFilter directories = FileFilterUtils.and(
                FileFilterUtils.directoryFileFilter(),
                HiddenFileFilter.VISIBLE);
        IOFileFilter files = FileFilterUtils.and(
                FileFilterUtils.fileFileFilter(),
                //set the listened file's suffix
                FileFilterUtils.suffixFileFilter(suffix));
        IOFileFilter filter = FileFilterUtils.or(directories, files);

        FileAlterationObserver observer = new FileAlterationObserver(file, filter);
        observer.addListener(new FileAlterationListenerAdaptorImpl(this.fileListeners));
        return new FileAlterationMonitor(INTERVAL, observer);
    }

    @Override
    public void start() {
        try {
            this.monitor.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean addFileListener(FileListener fileListener) {
        return this.fileListeners.add(fileListener);
    }

    @Override
    public boolean removeFileListener(FileListener fileListener) {
        return this.fileListeners.remove(fileListener);
    }

    @Override
    public void stop() {
        try {
            this.monitor.stop();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static final class FileAlterationListenerAdaptorImpl extends FileAlterationListenerAdaptor {

        public FileAlterationListenerAdaptorImpl(Set<FileListener> fileListeners) {
            this.fileListeners = fileListeners;
        }

        private final Set<FileListener> fileListeners;

        @Override
        public void onStart(final FileAlterationObserver observer) {
            logger.info("A new loop of monitoring for file is starting....");
        }

        @Override
        public void onDirectoryCreate(final File directory) {
            logger.info("Create a new directory. Directory: " + directory.getAbsolutePath());
        }

        @Override
        public void onDirectoryChange(final File directory) {
            logger.info("The directory is changed. Directory: " + directory.getAbsolutePath());
        }

        @Override
        public void onDirectoryDelete(final File directory) {
            logger.info("The directory is deleted. Directory: " + directory.getAbsolutePath());
        }

        @Override
        public void onFileCreate(File file) {
            logger.info("The file is created. File: " + file.getAbsolutePath());
        }

        @Override
        public void onFileChange(File file) {
            logger.info("The file is changed. File: " + file.getAbsolutePath());
            for (FileListener fileListener : this.fileListeners) {
                fileListener.onChanged(file);
            }
        }

        @Override
        public void onFileDelete(File file) {
            logger.info("The file is deleted. File: " + file.getAbsolutePath());
        }

        @Override
        public void onStop(final FileAlterationObserver observer) {
            logger.info("A loop is stopped.");
        }
    }
}
