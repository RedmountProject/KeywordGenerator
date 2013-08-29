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
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.sql.SQLException;
import java.util.*;

import org.apache.solr.client.solrj.SolrServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author amaury
 */
class Main {

    /**
     * @param args the command line arguments
     */
    //public static FileLogger fl = new FileLogger("/home/amaury/NetBeansProjects/KeywordGenerator/logs");
    public static final Logger LOG = LoggerFactory.getLogger(Main.class);
    public static boolean job = false;
    public static final Config conf = new Config();
    public static KeywordSet NormierKeywords = new KeywordSet();

    static {
        try {
            LOG.info("Loading " + conf.getProperty("unitex_shared_library"));
            System.load( conf.getProperty("pwd")+ "/"+conf.getProperty("unitex_shared_library"));
        } catch (UnsatisfiedLinkError e) {
            LOG.info("Native code library failed to load." + e);
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {

        try {

            job = Boolean.parseBoolean(conf.getProperty("job.print"));

            //fl.getLogger("KeywordGenerator.log");
            int nb_books = 0;

            ArrayList<Book> AllBooks = new ArrayList<>();
            //extract_content docs
            if (Boolean.parseBoolean(conf.getProperty("fetch.from.db"))) {
                DatabaseManager databaseManager = new DatabaseManager(conf);
                AllBooks = databaseManager.getBooks();
                databaseManager.close();
            }

            if (Boolean.parseBoolean(conf.getProperty("daily"))) {
                DatabaseManager databaseManager = new DatabaseManager(conf);
                NormierKeywords = (KeywordSet) databaseManager.getNormierKeywords().clone();
                nb_books = databaseManager.getBookNumber();
                databaseManager.close();
            }
            
            //get additionnal docs from solr : defqult request :
            if (Boolean.parseBoolean(conf.getProperty("fetch.from.solr"))) {
                LOG.info("Fetching from Solr");
                SolrManager sm = new SolrManager(conf);
                if (Boolean.parseBoolean(conf.getProperty("daily"))) {
                    sm.getCatalogDocs();
                }

                LOG.info("Fetching Additionnal Keywords");
                AllBooks = sm.getAdditionnalBooks(AllBooks);
                LOG.info("End Fetching Additionnal Keywords");
            }

            if (AllBooks.isEmpty()) {
                LOG.info("You Haven't any Book to process check your configuration for details");
                System.exit(1);
            }



            LOG.info("Book Number to manage : " + AllBooks.size());

            if (Boolean.parseBoolean(conf.getProperty("daily"))) {
                GenerateKeywords(AllBooks, conf);
            } else {
                nb_books = GenerateKeywords(AllBooks, conf);
                
            }


            if (Boolean.parseBoolean(conf.getProperty("advance.keyword.generation"))) {
                NormierKeywords(AllBooks, nb_books);
                //ApplyFactorFive(AllBooks);
            }


            if (job) {
                PrintResults(AllBooks);
            } else {
                if (LOG.isDebugEnabled()) {
                    PrintResults(AllBooks);
                }
                InsertInSolr(AllBooks);
                InsertInDb(AllBooks);

            }

        } catch (UnsupportedEncodingException ex) {
            LOG.error("", ex);
        } catch (SQLException | SolrServerException | ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
            LOG.error("", ex);
        } catch (IOException ex) {
            LOG.error("", ex);
        }


    }

    private static int GenerateKeywords(ArrayList<Book> AllBooks, Config conf) throws UnsupportedEncodingException {
        LOG.info("Begin Keyword Generation");
        UnitexParser unitexManager = new UnitexParser(conf);

        int size = AllBooks.size();
        int i = 0;
        for (Book ABook : AllBooks) {
            if (ABook.isNotEmpty()) {
                unitexManager.AddKeywords(ABook);
                LOG.info("Generating Keywords for Book : " + i + "/" + size + " BookId : " + ABook.book_id);
                i++;
            }
        }
        LOG.info("End Keyword Generation");

        return size;
    }

    private static void InsertInDb(ArrayList<Book> AllBooks) throws Exception {

        LOG.info("Begin Keyword Insertion into Database");
        DatabaseManager databaseManager = new DatabaseManager(conf);
        databaseManager.insert_Keywords(NormierKeywords);
        NormierKeywords = null;
        databaseManager.insert_book_keywords(AllBooks);
        LOG.info("End Keyword Insertion into Database");
    }

    private static void InsertInSolr(ArrayList<Book> AllBooks) throws MalformedURLException, SolrServerException, IOException {
        LOG.info("Begin Keyword Insertion into Solr");
        SolrManager solrManager = new SolrManager(conf);
        solrManager.insert_Keywords(AllBooks);
        LOG.info("End Keyword Insertion into Solr");
    }

    private static void PrintResults(ArrayList<Book> AllBooks) {
        for (Book book : AllBooks) {
            book.Dump();
        }
    }

    /**
     * Specifig Improvement proposed by Normier. (sort Keyword by final computed
     * weight) This function is the only one whose change the weight of each
     * keyword in books.
     *
     * @param AllBooks List of Book Object UnitexParsed
     * @param size size of the list returned by GenerateKeywords (may be similar
     * with AllBooks.size() though useless here)
     */
    private static void NormierKeywords(ArrayList<Book> AllBooks, int size) {
        double factor;
        int nb_match;
        Double kw_total_weight;
        int total_weights = NormierKeywords.sum;

        LOG.info("Begin Normier Keywords");

        int i = 0;
        int len = AllBooks.size();
        for (Book b : AllBooks) {
            LOG.info("Normier Keywords for Book " + b.book_id + ", " + i + "/" + len);
            LOG.debug("Size  : " + size);
            i++;
            for (Keyword kw : b.keywords) {

//                fl.info("Keyword " + kw.keyword + "." + kw.lemma + " match : "
//                        + keyword_match_count_mapping.get(kw.keyword + "." + kw.lemma)
//                        + " times and has a total weight of : "
//                        + keyword_weights_mapping.get(kw.keyword + "." + kw.lemma));
                LOG.debug("Keyword " + kw.keyword + "." + kw.lemma + " match : "
                        + NormierKeywords.getglobal_count(kw)
                        + " times and has a total weight of : "
                        + NormierKeywords.getWeight(kw));

                nb_match = NormierKeywords.getglobal_count(kw);

                factor = 1 - ((double) nb_match / size);
                LOG.debug("Factor : " + factor);
                LOG.debug("Keyword Current Weight : " + kw.weight);
                LOG.debug("Book nb_Corpus : " + b.nb_corpus);

                kw.weight *= factor / b.nb_corpus;
                kw_total_weight = NormierKeywords.getWeight(kw);

                factor = 1 / ((double) kw_total_weight / total_weights);

                LOG.debug("Factor : " + factor);

                kw.weight *= factor;
                //System.out.print(kw.keyword+"  :"+kw.weight+"\n");
                kw.weight += 1.0;

                LOG.debug("Factor At Last : " + factor);
                LOG.debug("Keyword Current Weight At Last : " + kw.weight);


                //System.out.print(kw.getConcat()+"\n");
            }
        }
        LOG.info("End Normier Keywords");
    }
}
