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

import java.math.BigDecimal;
import java.text.Normalizer;
import java.util.Objects;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author amaury
 */
public class Keyword {

    public static final Logger LOG = LoggerFactory.getLogger(Keyword.class.getName());
    public String keyword;
    public Integer id = -1;
    public String lemma;
    public String concat;
    public Double weight = 1.0;
    public int global_weight;
    public int global_count;

    public Keyword(String kString, String lString, Double wI) {
        keyword = stripAccents(kString);
        lemma = lString;
        weight = (wI == null) ? 1.0 : wI;
    }

    public Keyword(String pKeyword, String pLemma, Integer pGlobal_weight, int pGlobal_count) {
        keyword = stripAccents(pKeyword);
        LOG.debug("Creating keyword : "+ keyword+" Gloval_Weight : "+pGlobal_weight+" Global_count "+pGlobal_count);
        lemma = pLemma;
        global_weight = (pGlobal_weight == 0) ? 1 : pGlobal_weight;
        global_count = pGlobal_count;
    }
    
    public Keyword(int pId, String kString, String pString, Double pWeight) {
        id = pId;
        keyword = stripAccents(kString);
        LOG.debug("Creating keyword : "+ keyword);
        lemma = pString;
        weight = (pWeight == null) ? 1.0 : pWeight;
    }

    public Keyword(int pId,String pKeyword, String pLemma, int pGlobal_weight, int pGlobal_count) {
        id = pId;
        keyword = stripAccents(pKeyword);
        lemma = pLemma;
        weight = 1.0;
        global_weight = pGlobal_weight;
        global_count = pGlobal_count;
    }

    public String getStringWeight() {
        return String.format("%.2f", new Float(weight));
    }

    public void dump() {
        System.out.println("Keyword : " + keyword + "  Lemma : " + lemma + "  Weight : " + weight);
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 41 * hash + Objects.hashCode(keyword);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        if (obj.hashCode() == hashCode()) {
            return true;
        }
        return false;
    }

    String getConcat() {
        return this.keyword + "." + this.lemma + "|" + getStringWeight();
    }

    synchronized  void setId(int kw_id) {
        this.id = kw_id;
    }
    
    private static final String[] InputReplace =  {"é", "è", "ê", "ë", "û", "ù", "ü", "ï", "î", "à", "â", "ö", "ô", "ç" };
    private static final String[] OutputReplace = {"e", "e", "e", "e", "u", "u", "u", "i", "i", "a", "a", "o", "o", "c" };
    public static String stripAccents(String s) {
        s = StringUtils.replaceEachRepeatedly(s.toLowerCase(), InputReplace, OutputReplace);
        s = StringEscapeUtils.escapeSql(s);
        s = Normalizer.normalize(s.toLowerCase(), Normalizer.Form.NFD);
        return s;
    }
}
