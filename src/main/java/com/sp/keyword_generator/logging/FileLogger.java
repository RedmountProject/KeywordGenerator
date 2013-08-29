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
package com.sp.keyword_generator.logging;

import java.io.IOException;
import java.util.logging.*;

/**
 *
 * @author amaury
 */
public class FileLogger {

    private String log_path;
    private String main_path;
    private FileHandler fh;

    /**
     * Create a FileLogger instance with only basic String representing the root
     * directory for logging
     *
     * @param log_path Root Directory for logging into
     * @throws IOException
     * @see getLogger() for FileHandler creation
     */
    public FileLogger(String log_path) {
        this.log_path = log_path;
         this.main_path = log_path;
    }
    
    public void reset(){
        this.log_path = this.main_path;
    }
    
    public FileLogger(String log_path, FileHandler fh) {
        this.log_path = log_path;
        this.fh = fh;
    }

    public String getPath() {
        return this.log_path;
    }

    /**
     * Allow you to update the root directory for logging into
     *
     * @param log_path new directory string for logging
     */
    public void updateLogger(String log_path) {
        this.log_path = log_path;
    }

    /**
     * Allow you to append 'log_path' the root directory for logging into
     *
     * @param log_path String to append to the current directory path for
     * logging
     */
    public void appendLogger(String log_path) {
        this.log_path = this.log_path+"/"+log_path;
    }

    /**
     * Create a file Handler with file param
     *
     * @param file a String representing the file to log into
     * @return FileHandler
     */
    public void getLogger(String file) {
        try {
            if(fh != null){
                fh.close();
            }
            fh = new FileHandler(log_path + "/" + file);
            fh.setFormatter(new Formatter() {

                @Override
                public String format(LogRecord record) {
                    return record.getMessage()+"\n";
                }
            });
            
        } catch (IOException | SecurityException ex) {
            java.util.logging.Logger.getLogger(this.getClass().getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Write info
     *
     * @param Line
     */
    public void info(String Line) {
        fh.publish(new LogRecord(Level.INFO, Line));
    }

    public void Log(Level log_lvl, String Line) {
        fh.publish(new LogRecord(log_lvl, Line));
    }
}
