/*  Copyright (C) 2013 BRISOU Amaury

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License along
    with this program; if not, write to the Free Software Foundation, Inc.,
    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
package com.sp.keyword_generator;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Jdbc implements Closeable {

    public static final Logger LOG = Logger.getLogger(Jdbc.class.getName());

    @Override
    public void close() throws IOException {
       this.close();
    }

    public enum AUTO_COMMIT {

        FALSE, TRUE;
    }

    public static java.sql.Connection Connection(String DatabaseAddress, String DatabasePort, String DatabaseName,
            String DatabaseUserName, String DatabasePassword, final boolean AUTO_COMMIT) throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {


        String DatabaseUrl = "jdbc:mysql://" + DatabaseAddress + ":" + DatabasePort + "/" + DatabaseName + "?user=" + DatabaseUserName
                + "&password=" + DatabasePassword;

        LOG.log(Level.INFO, "{0}", "Connecting to database : " + DatabaseUrl);
        Connection connect;
        
        Class.forName("com.mysql.jdbc.Driver").newInstance(); //JdbcOdbcDriver
        connect = DriverManager.getConnection(DatabaseUrl);
        connect.setAutoCommit(AUTO_COMMIT);
        LOG.log(Level.INFO, "{0}", "Connection Established");



        return connect;
    }
}
