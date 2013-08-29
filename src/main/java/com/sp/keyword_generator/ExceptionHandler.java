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

import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author amaury
 */
public class ExceptionHandler extends Exception {

    public enum Errors {

        AddUrlException("Instance couldn't inject Url into database"),
        UnsatisfiedLinkError("Couldn't Find Unitex Native Library"),
        ConnectException("Couldn't Connect To Mysql Server"),
        CommunicationsException("Communications link failure"),
        IOException("Input Ouput : FileSystem or Network Error"),
        BoilerPipeException("BoilerPipe Error"),
        InterruptedException("InterruptedException"),
        IllegalAccessException("IllegalAccessException"),
        ClassNotFoundException("ClassNotFoundException"),
        InstantiationException("InstantiationException");
        private String message;

        private Errors(final String message) {
            this.message = message;
        }

        public String toString() {
            return message;
        }
    }

    public ExceptionHandler(Throwable ex) throws Exception {
        super(ex);
        Logger.getLogger(super.getClass().getName()).log(Level.SEVERE, ExceptionHandler.Errors.AddUrlException.toString(), ex);
    }

    public ExceptionHandler(SQLException ex) {
        super(ex);
        Logger.getLogger(super.getClass().getName()).log(Level.SEVERE, ExceptionHandler.Errors.ConnectException.toString(), ex);
    }

    public ExceptionHandler(com.mysql.jdbc.exceptions.jdbc4.CommunicationsException ex) {
        super(ex);
        Logger.getLogger(super.getClass().getName()).log(Level.SEVERE, ExceptionHandler.Errors.CommunicationsException.toString(), ex);
    }

    public ExceptionHandler(IOException ex) {
        super(ex);
        Logger.getLogger(super.getClass().getName()).log(Level.SEVERE, ExceptionHandler.Errors.IOException.toString(), ex);
    }


    public ExceptionHandler(InterruptedException ex) {
        super(ex);
        Logger.getLogger(super.getClass().getName()).log(Level.SEVERE, ExceptionHandler.Errors.InterruptedException.toString(), ex);
    }

    public ExceptionHandler(IllegalAccessException ex) {
        super(ex);
        Logger.getLogger(super.getClass().getName()).log(Level.SEVERE, ExceptionHandler.Errors.IllegalAccessException.toString(), ex);
    }

    public ExceptionHandler(ClassNotFoundException ex) {
        super(ex);
        Logger.getLogger(super.getClass().getName()).log(Level.SEVERE, ExceptionHandler.Errors.ClassNotFoundException.toString(), ex);
    }

    public ExceptionHandler(InstantiationException ex) {
        super(ex);
        Logger.getLogger(super.getClass().getName()).log(Level.SEVERE, ExceptionHandler.Errors.InstantiationException.toString(), ex);
    }

    public ExceptionHandler(Exception ex) {
        Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
    }
}
