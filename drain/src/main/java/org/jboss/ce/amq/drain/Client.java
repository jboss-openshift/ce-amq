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

import java.io.Closeable;
import java.io.IOException;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * @author <a href="mailto:ales.justin@jboss.org">Ales Justin</a>
 */
public abstract class Client implements Closeable {
    private String url;
    private String username;
    private String password;

    protected Connection connection;
    protected Session session;

    private JMX jmx;

    public Client(String url, String username, String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    protected ConnectionFactory getConnectionFactory() {
        return new ActiveMQConnectionFactory(url);
    }

    public void close() throws IOException {
        try {
            close(connection);
        } catch (JMSException ignored) {
        }
    }

    protected void close(Connection connection) throws JMSException {
        if (connection != null) {
            connection.close();
        }
    }

    public void start() throws JMSException {
        ConnectionFactory cf = getConnectionFactory();
        connection = cf.createConnection(username, password);
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        connection.start();
    }

    public void stop() throws JMSException {
        connection.stop();
    }

    public JMX getJMX() {
        if (jmx == null) {
            jmx = new JMX();
        }
        return jmx;
    }
}
