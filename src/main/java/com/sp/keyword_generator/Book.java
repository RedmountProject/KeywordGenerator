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

import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author amaury
 */
class Book {

    public String content;
    public final int book_id;
    public final int nb_corpus;
    public KeywordSet keywords = new KeywordSet();
    public ArrayList<String[]> book_infos = new ArrayList<>();

    public Book(String pContent, int pId, int pNb_corpus, ArrayList<String[]> pBook_infos) {
        content = filterContent(pContent);
        book_id = pId;
        book_infos = pBook_infos;
        nb_corpus = pNb_corpus;
    }

    public void addContent(String pContent) {
        content += pContent;
    }

    public void addKeyword(Keyword pKw) {
        keywords.put(pKw);
    }

    public void setKeywords(KeywordSet pKws) {
        keywords = (KeywordSet) pKws.clone();
    }
    
    public void addKeywords(KeywordSet pKws) {
        for (Keyword kw : pKws) {
            keywords.put(kw);
        }
    }

    public void Dump() {

        log("################# BEGIN DUMP ################");
        log(" id = " + book_id);

        for (Keyword kw : keywords) {
            kw.dump();
        }
        log(" content = " + content);
        log("################# END DUMP ################");

    }

    private void log(String s) {
        System.out.println("LOG :" + s);
    }

    public boolean isNotEmpty() {
        if (this.content.isEmpty()
                && this.keywords.isEmpty()
                && this.book_id == 0) {
            return false;
        }
        return true;
    }

    protected void sleep(int i) {
        try {
            Thread.sleep(i);
        } catch (InterruptedException ex) {
            Logger.getLogger(Book.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private String filterContent(String content) {
        String firstname_lastname, title, lastname;
        for (String[] b_info : book_infos) {
            firstname_lastname = b_info[0] + " " + b_info[1];
            content = content.replaceAll("\\b" + firstname_lastname + "\\b", "");
            lastname = b_info[1];
            content = content.replaceAll("\\b" + lastname + "\\b", "");
            title = b_info[2];
            content = content.replaceAll("\\b" + title + "\\b", "");
        }

        content = content.replaceAll("\\<.*?\\>", "");

        return content;
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }


    @Override
    public int hashCode() {
        int hash = 5;
        hash = 97 * hash + this.book_id;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if(obj.getClass() != Integer.class){
            return false;
        }
        final Integer other = (Integer) obj;
        if (this.book_id != book_id) {
            return false;
        }
        return true;
    }
 
    
    
}
