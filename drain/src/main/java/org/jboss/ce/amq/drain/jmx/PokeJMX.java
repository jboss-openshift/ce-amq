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

package org.jboss.ce.amq.drain.jmx;

import java.util.logging.Logger;

import javax.management.ObjectInstance;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public class PokeJMX extends AbstractJMX {
    private static final Logger log = Logger.getLogger(PokeJMX.class.getName());

    public static void main(String[] args) throws Exception {
        if (args == null || args.length == 0) {
            System.out.println(System.getProperties());
            System.out.println("------------");
            System.out.println("Sleeping ...");
            Thread.sleep(10 * 60 * 1000L);
        } else {
            PokeJMX jmx = new PokeJMX();
            jmx.list(args);
        }
    }

    private void list(String[] args) throws Exception {
        for (ObjectInstance oi : queryMBeans(createJmxConnection(), args[0])) {
            print(oi.getObjectName().getCanonicalName());
        }
    }

    protected void print(String msg) {
        log.info(msg);
    }
}
