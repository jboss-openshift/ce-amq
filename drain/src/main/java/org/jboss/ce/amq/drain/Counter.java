/*
 * JBoss, Home of Professional Open Source
 * Copyright 2016 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.ce.amq.drain;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
class Counter implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(Counter.class);

    private Map<String, Integer> sizes = new HashMap<>();
    private Map<String, Integer> counters = new HashMap<>();

    void setSize(String destination, int size) {
        sizes.put(destination, size);
    }

    void increment(String destination) {
        Integer x = counters.get(destination);
        if (x == null) {
            x = 0;
        }
        x++;
        counters.put(destination, x);
    }

    public void run() {
        StringBuilder builder = new StringBuilder();
        builder.append("A-MQ counter: \n");
        for (Map.Entry<String, Integer> entry : counters.entrySet()) {
            builder
                .append(String.format("'%s'", entry.getKey()))
                .append(" -> ").append(entry.getValue()).append(" / ").append(sizes.get(entry.getKey()))
                .append("\n");
        }
        log.info(builder.toString());
    }
}
