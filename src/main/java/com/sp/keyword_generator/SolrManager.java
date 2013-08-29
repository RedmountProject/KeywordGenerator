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
import java.net.MalformedURLException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.util.ClientUtils;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SolrManager {

    public static final Logger LOG = LoggerFactory.getLogger(SolrManager.class.getName());
    SolrServer solr;
    private static int MAX_FETCH_ROWS;
    private final int COMMIT_SIZE;
    private final Config conf;

    public SolrDocumentList getCatalogDocs() throws SolrServerException {

        String SolrAdditionnalQuery = conf.getProperty("solr.additionnal.query");
        SolrQuery solrQuery = new SolrQuery().setQuery(SolrAdditionnalQuery).setRows(MAX_FETCH_ROWS);

        QueryResponse rsp = solr.query(solrQuery);

        return rsp.getResults();
    }

    private Iterator<SolrDocument> getSolrBookIterator(int pBookId) throws SolrServerException {


        SolrQuery solrQuery = new SolrQuery().setQuery("book_id:" + pBookId).setRows(MAX_FETCH_ROWS);

        QueryResponse rsp = solr.query(solrQuery);

        return rsp.getResults().iterator();
    }

    ArrayList<Book> getAdditionnalBooks(ArrayList<Book> AllBooks)
            throws SolrServerException, MalformedURLException, SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {

        
        SolrDocumentList docs = getCatalogDocs();

        int bookid;
        Book ABook = null;
        boolean book_exists;
        String content;
        DatabaseManager databaseManager = new DatabaseManager(conf);

        for (SolrDocument CurrentSolrDoc : docs) {
            book_exists = false;
            try {
                bookid = Integer.parseInt(CurrentSolrDoc.getFieldValue("book_id").toString());
                content = CurrentSolrDoc.getFieldValue("description").toString();

                KeywordSet set = databaseManager.getOnixKeywords(bookid);
                for (Book book : AllBooks) {
                    if (book.book_id == bookid) {
                        book_exists = true;
                        ABook = book;
                    }
                }
                if (book_exists) {
                    ABook.addKeywords(set);
                } else {
                    Book Abook = new Book(content, bookid, 1, null);
                    Abook.addKeywords(set);
                    AllBooks.add(Abook);
                }

            } catch (SQLException ex) {
                LOG.error("", ex);
            }
        }

        databaseManager.close();

        return AllBooks;
    }

    SolrManager(Config pConf) throws MalformedURLException, SolrServerException, IOException {
        this.conf = pConf;
        String SolrUrl = conf.getProperty("solr.url");
        MAX_FETCH_ROWS = Integer.parseInt(conf.getProperty("solr.fetch.size"));
        COMMIT_SIZE = Integer.parseInt(conf.getProperty("solr.commit.size"));
        solr = new HttpSolrServer(SolrUrl);
    }

    void insert_Keywords(ArrayList<Book> AllBooks) throws SolrServerException, MalformedURLException, IOException {
        Integer InputListSize;
        SolrInputDocument InputDoc;
        int CurrentSolrDocId;
        int len = AllBooks.size();
        ArrayList<SolrInputDocument> InputDocList = new ArrayList<>();
        Collection<Object> LastNameArray;
        SolrDocument CurrentSolrDoc;
        for (Book ABook : AllBooks) {

            Iterator<SolrDocument> solrDocIt = getSolrBookIterator(ABook.book_id);
            while (solrDocIt.hasNext()) {
                CurrentSolrDoc = solrDocIt.next();

                if (CurrentSolrDoc == null) {
                    continue;
                }
                
                CurrentSolrDocId = Integer.parseInt(CurrentSolrDoc.getFieldValue("book_id").toString());

                if (ABook.isNotEmpty() && CurrentSolrDocId == ABook.book_id) {

                    LastNameArray = CurrentSolrDoc.getFieldValues("author_lastname");
                    CurrentSolrDoc.removeFields("keywords");

                    for (Keyword kw : ABook.keywords) {
                        LOG.debug("Adding "+kw.getConcat());
                        //if(kw.keyword.equalsIgnoreCase("genre litteraire") || kw.keyword.equalsIgnoreCase("romance")) continue;
                        CurrentSolrDoc.addField("keywords", kw.getConcat());
                    }

                    //At Last add the author_lastname(s) as keyword with lemma NA and weight : 1
                    if (LastNameArray != null) {
                        for (Iterator it = LastNameArray.iterator(); it.hasNext();) {
                            CurrentSolrDoc.addField("keywords", ((String) it.next()).toLowerCase() + ".NA|1");
                        }
                    }

                    //CurrentSolrDoc.setField("searchable", true);

                    InputDoc = ClientUtils.toSolrInputDocument(CurrentSolrDoc);

                    InputDocList.add(InputDoc);
                    InputListSize = InputDocList.size();
                    if (InputListSize >= COMMIT_SIZE) {
                        solr.add(InputDocList);
                        solr.commit();
                        InputDocList.clear();
                    }
                    LOG.info("Book Added To solr Queue " + InputListSize + "/" + COMMIT_SIZE + "/" + len + " : " + ABook.book_id);
                }

            }
        }
        solr.add(InputDocList);
        solr.commit();
    }
}