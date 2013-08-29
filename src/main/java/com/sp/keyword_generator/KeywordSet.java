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

import java.util.*;
import org.apache.log4j.Logger;

/**
 *
 * @author amaury
 */
class KeywordSet extends ArrayList<Keyword> {

    Logger LOG = Logger.getLogger(this.getClass());
    private HashSet<Integer> _this = new HashSet<>();
    private List<Integer> book_ids = new ArrayList<>();
    public int sum = 0;

    public void increment(Keyword pKw) {
        get(indexOf(pKw)).global_count++;
        LOG.debug("Keyword : " + pKw.getConcat() + " global_count : " + get(indexOf(pKw)).global_count);
    }

    public int getglobal_count(Keyword pKw) {
        if (contains(pKw)) {
            return get(indexOf(pKw)).global_count;
        }
        return -1;
    }

    public Double getWeight(Keyword pKw) {
        if (contains(pKw)) {
            return get(indexOf(pKw)).weight;
        }
        return -1.0;
    }

    public int getSumWeight(Keyword pKw) {
        if (contains(pKw)) {
            return get(indexOf(pKw)).global_weight;
        }
        return -1;
    }

    private void addWeight(Keyword pKw) {

        get(indexOf(pKw)).global_weight += pKw.weight;
        sum += pKw.weight;

        LOG.debug("Keyword : " + pKw.getConcat() + " weight : " + get(indexOf(pKw)).weight);
        LOG.debug("Keyword : " + pKw.getConcat() + " global_weight : " + get(indexOf(pKw)).global_weight);
        LOG.debug("Sum : " + sum);

    }

    /**
     * Book Put method
     *
     * @param pKw
     * @return
     */
    public synchronized boolean put(Keyword pKw) {

        if (_this.add(pKw.hashCode())) {
            LOG.debug("Adding to Book " + pKw.keyword);
            add(pKw);

            increment(pKw);
            addWeight(pKw);

            return true;
        } else {
            LOG.debug("Keyword " + pKw.keyword + " Already Exists : Skipping");
            return false;
        }
    }

    /**
     * Normier Keyword put Method
     *
     * @param book_id
     * @param pKw
     * @return
     */
    public synchronized boolean put(int book_id, Keyword pKw) {

        if (_this.add(pKw.hashCode())) {
            LOG.debug("Adding to NormierKeywords " + pKw.keyword + " for book_id : " + book_id);
            add(pKw);

            increment(pKw);
            addWeight(pKw);
            book_ids.add(book_id);

            return true;
        } else if (!book_ids.contains(book_id)) {
            LOG.debug("Keyword " + pKw.keyword + " doesn't Exists for book_id : " + book_id + " : incrementing and adding current weight");
            increment(pKw);
            addWeight(pKw);
            return true;
        } else {
            LOG.debug("Keyword " + pKw.keyword + " Already Exists for book_id : " + book_id + " : Skipping");
            return false;
        }
    }

    public void addAll(KeywordSet set) {
        for (Keyword keyword : set) {
            put(keyword);
        }
    }
}
