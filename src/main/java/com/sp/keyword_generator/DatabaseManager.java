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

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import org.slf4j.Logger;


import org.slf4j.LoggerFactory;

public class DatabaseManager {

    public static final Logger LOG = LoggerFactory.getLogger(DatabaseManager.class.getName());
    Jdbc s = new Jdbc();
    ResultSet rs;
    private Connection FetchCon, InsertCon;
    PreparedStatement fetchOnixKeyword;
    private final Integer DB_FETCH_SIZE;
    private String DatabaseName;
    private final Config conf;
    private final HashMap<String, Integer> lemmas;

    DatabaseManager(Config conf) throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        this.conf = conf;


        DB_FETCH_SIZE = Integer.parseInt(conf.getProperty("db.fetch.size"));

        boolean db = Boolean.parseBoolean(conf.getProperty("fetch.from.db"));
        if (db) {
            //FetchConnection();
        }
        InsertConnection();
        fetchOnixKeyword = getFetchKeywordStatement();
        lemmas = loadLemmas();
    }

    private void FetchConnection() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {

        String DatabaseAddress = conf.getProperty("in.db.address");
        String DatabasePort = conf.getProperty("in.db.port");
        String db_name = conf.getProperty("in.db.name");
        String DatabaseUserName = conf.getProperty("in.db.username");
        String DatabasePassword = conf.getProperty("in.db.password");

        FetchCon = Jdbc.Connection(DatabaseAddress, DatabasePort, db_name, DatabaseUserName, DatabasePassword, false);

    }

    private void InsertConnection() throws ClassNotFoundException, InstantiationException, IllegalAccessException, SQLException {

        String DatabaseAddress = conf.getProperty("out.db.address");
        String DatabasePort = conf.getProperty("out.db.port");
        DatabaseName = conf.getProperty("out.db.name");
        String DatabaseUserName = conf.getProperty("out.db.username");
        String DatabasePassword = conf.getProperty("out.db.password");

        InsertCon = Jdbc.Connection(DatabaseAddress, DatabasePort, DatabaseName, DatabaseUserName, DatabasePassword, false);

    }

    void insert_Keywords(KeywordSet set) {
        try {
            int kw_id;
            int COMMIT_SIZE = Integer.parseInt(conf.getProperty("db.commit.size"));

            PreparedStatement insertKeyword = InsertKeywordStatement();
            PreparedStatement updateKeyword = UpdateKeywordStatement();
            PreparedStatement fetchKewordId = FetchKeywordIdStatement();

            LOG.info("Inserting Keywords");
            LOG.info("COMMIT_SIZE : " + COMMIT_SIZE);
            int c_insert = 0, c_update = 0, c_commit = 0;
            for (Keyword keyword : set) {
                try {
                    insertKeyword.setString(1, keyword.keyword);
                    insertKeyword.setString(2, keyword.keyword);
                    insertKeyword.setInt(3, keyword.global_count);
                    insertKeyword.setDouble(4, keyword.global_weight);
                    insertKeyword.setInt(5, lemmas.get(keyword.lemma));

                    insertKeyword.addBatch();

                    c_insert++;

                    if (c_insert >= COMMIT_SIZE) {
                        LOG.info("Executing Batch : Keywords to Database");
                        insertKeyword.executeBatch();
                        c_insert = 0;
                        insertKeyword.clearBatch();
                    }

                } catch (BatchUpdateException ex) {
                    LOG.info(ex.getMessage());


                    kw_id = getKeywordId(fetchKewordId, keyword);
                    keyword.setId(kw_id);

                    LOG.info("Updating Keyword Global_Count && Global_Weight ");

                    updateKeyword.setInt(1, keyword.global_count);
                    updateKeyword.setFloat(2, keyword.global_weight);
                    updateKeyword.setInt(3, kw_id);
                    updateKeyword.addBatch();

                    c_update++;

                    LOG.debug("Relation between Keyword id : " + kw_id + " and Lemma id : " + lemmas.get(keyword.lemma) + " Already exists : Transaction is being rolled back");

                    LOG.debug("Updating Keyword Global_Count && Global_Weight ");

                    if (c_update >= COMMIT_SIZE) {
                        LOG.info("Executing Batch : Update Keywords to Database");
                        updateKeyword.executeBatch();
                        updateKeyword.clearBatch();
                        c_update = 0;
                    }
                }
                c_commit++;
                if (c_commit >= COMMIT_SIZE * 5) {
                    LOG.info("Committing !");
                    InsertCon.commit();
                }
            }
            LOG.info("Committing Keywords to Database");
            insertKeyword.executeBatch();
            if (c_update > 0) {
                updateKeyword.executeBatch();
            }
            InsertCon.commit();
        } catch (SQLException ex) {
            LOG.info(ex.getMessage());
        }
    }

    private PreparedStatement InsertKeywordStatement() throws SQLException {

        String requete = "INSERT INTO keyword ( keyword_id, name, lemmatized_name,  global_count, global_weight, lemma_type_id_fk )"
                + " VALUES((SELECT IFNULL( MAX( keyword_id ) , 0 )+1 FROM keyword target), ?, ? , ?, ?, ? );";
        // + kw.keyword + "', '" + kw.keyword + "', " + Main.NormierKeywords.getglobal_count(kw) + ", " + Main.NormierKeywords.getSumWeight(kw) + ", " + lemma_id + " );";
        return InsertCon.prepareStatement(requete, Statement.RETURN_GENERATED_KEYS);
    }

    private PreparedStatement UpdateKeywordStatement() throws SQLException {
        String requete = "UPDATE keyword set global_count = ?, global_weight = ? WHERE keyword_id = ?";
        //+ kw.global_count + " , global_weight = " + kw.global_weight + " WHERE keyword_id = " + kw_id;
        return InsertCon.prepareStatement(requete);
    }

    void insert_book_keywords(ArrayList<Book> AllBooks) {
        try {
            int len = AllBooks.size();
            PreparedStatement insertBookKeyword = InsertBookKeywordStatement();
            PreparedStatement updateBookKeyword = UpdateBookKeywordStatement();
            PreparedStatement fetchKewordId = FetchKeywordIdStatement();

            int COMMIT_SIZE = Integer.parseInt(conf.getProperty("db.commit.size"));
            int kw_id, c_insert = 0, c_book = 0, c_update = 0, c_commit = 0;

            for (Book book : AllBooks) {
                LOG.info("Inserting Book_Keywords Relations : " + c_book + "/" + len + " Book_id :" + book.book_id);
                c_book++;
                for (Keyword keyword : book.keywords) {
                    try {
                        kw_id = getKeywordId(fetchKewordId, keyword);

                        insertBookKeyword.setInt(1, book.book_id);
                        insertBookKeyword.setInt(2, kw_id);
                        insertBookKeyword.setString(3, keyword.getStringWeight());
                        insertBookKeyword.addBatch();

                        c_insert++;

                        if (c_insert >= COMMIT_SIZE) {
                            LOG.info("Executing Batch : Book_Keywords relations to Database ");
                            insertBookKeyword.executeBatch();
                            insertBookKeyword.clearBatch();
                            c_insert = 0;
                        }
                    } catch (BatchUpdateException ex) {
                        LOG.debug(ex.getMessage());
                        try {

                            kw_id = getKeywordId(fetchKewordId, keyword);

                            LOG.debug("Updating Book_Keyword Relation book_id ; " + book.book_id + " and keyword_id : " + kw_id);

                            updateBookKeyword.setInt(1, book.book_id);
                            updateBookKeyword.setInt(2, kw_id);
                            updateBookKeyword.setString(3, keyword.getStringWeight());
                            updateBookKeyword.addBatch();

                            c_update++;

                            if (c_update >= COMMIT_SIZE) {
                                LOG.debug("Executing Batch Update : Book_Keywords relations to Database");
                                updateBookKeyword.executeBatch();
                                c_update = 0;
                                InsertCon.rollback();
                                updateBookKeyword.clearBatch();
                            }

                        } catch (BatchUpdateException excep) {
                            LOG.debug(excep.getMessage());
                        }
                    }
                    c_commit++;
                    if (c_commit >= COMMIT_SIZE * 5) {
                        LOG.info("Committing !");
                        InsertCon.commit();
                        c_commit = 0;
                    }
                }
                book = null;
            }
            LOG.info("Committing Book_Keywords relations to Database");
            insertBookKeyword.executeBatch();
            if (c_update > 0) {
                updateBookKeyword.executeBatch();
            }
            InsertCon.commit();
        } catch (SQLException ex) {
            LOG.info(ex.getMessage());
        }
    }

    private PreparedStatement InsertBookKeywordStatement() throws SQLException {
        String requete = "INSERT INTO book_keyword  ( book_id_fk, keyword_id_fk, keyword_weight) VALUES ( ?, ?, ? )";
        //+ id_book + "', '" + kw.id + "' , '" + kw.getStringWeight() + "' );";
        return InsertCon.prepareStatement(requete);
    }

    private PreparedStatement FetchKeywordIdStatement() throws SQLException {
        return InsertCon.prepareStatement("SELECT keyword_id FROM keyword WHERE name like ?;");
    }

    private PreparedStatement UpdateBookKeywordStatement() throws SQLException {
        String requete = "UPDATE book_keyword  SET keyword_weight = ? WHERE book_id_fk = ? AND keyword_id_fk = ?";
        //+ id_book + "', '" + kw.id + "' , '" + kw.getStringWeight() + "' );";
        return InsertCon.prepareStatement(requete);
    }

    private HashMap<String, Integer> loadLemmas() throws SQLException {
        PreparedStatement insertLemma = InsertCon.prepareStatement("SELECT lemma_type_id, name  FROM lemma_type;");
        rs = insertLemma.executeQuery();
        int lemma_id = -1;
        HashMap<String, Integer> ret = new HashMap<>(10);
        while (rs.next()) {
            ret.put(rs.getString("name"), rs.getInt("lemma_type_id"));
        }

        return ret;
    }

    private int getLemmaId(String lemma) throws SQLException {

        PreparedStatement insertLemma = InsertCon.prepareStatement("SELECT lemma_type_id FROM `" + DatabaseName + "`.`lemma_type` WHERE `name` like '" + lemma + "';");
        rs = insertLemma.executeQuery();
        int lemma_id = -1;
        while (rs.next()) {
            lemma_id = rs.getInt(1);
        }

        return lemma_id;
    }

    private int insert_Lemma(String lemma) throws SQLException {

        LOG.debug("inserting lemma : " + lemma);


        String requete = "INSERT INTO `" + DatabaseName + "`.`lemma_type` ( `lemma_type_id`, `name` )"
                + " VALUES((SELECT IFNULL( MAX( lemma_type_id ) , 0 )+1 FROM `" + DatabaseName + "`.`lemma_type` target), '" + lemma + "');";
        PreparedStatement insertLemma = null;
        int lemma_id = -1;
        try {

            insertLemma = InsertCon.prepareStatement(requete);
            insertLemma.execute();
            InsertCon.commit();

            lemma_id = getLemmaId(lemma);

            LOG.debug("lemma : " + lemma + " id : " + lemma_id + " inserted");
        } catch (SQLIntegrityConstraintViolationException e) {
            if (InsertCon != null) {
                try {

                    lemma_id = getLemmaId(lemma);

                    LOG.debug("Lemma " + lemma + " Already exists : id : " + lemma_id);

                    InsertCon.rollback();

                } catch (SQLException excep) {
                    LOG.error("Error inserting Lemma : ", excep);
                }
            }
        } finally {
            if (insertLemma != null) {
                insertLemma.close();
            }
            return lemma_id;
        }

    }
    
    Integer getBookNumber(){
        try {
            String requete = "SELECT count(*) FROM book;";

            PreparedStatement statement = InsertCon.prepareStatement(requete);
            rs = statement.executeQuery();
            while(rs.next()){
                LOG.debug("Fetch Size : "+rs.getInt(1));
                return rs.getInt(1);
            }
        } catch (SQLException ex) {
            LOG.error("mmm", ex);
        }
        return 0;
    }

    ArrayList<Book> getBooks() throws SQLException {

        ArrayList<Book> AllBooks = new ArrayList<>();

        LOG.info("Fetching Book in " + conf.getProperty("in.db.name") + " Database");
        String requete = "SELECT `book_id`, `content`, `nb_match` FROM `book_content` AND nb_match < 50 ORDER BY `book_id` LIMIT " + DB_FETCH_SIZE;

        PreparedStatement statement = FetchCon.prepareStatement(requete);
        rs = statement.executeQuery();
        String content = "";
        String description = "";
        int book_id, nb_corpus;
        ArrayList<String[]> book_infos;

        while (rs.next()) { // process results one row at a time
            if (rs.getString(2) != null) {
                content = rs.getString(2);
            }
            book_id = rs.getInt(1);

            book_infos = getBookInfos(book_id);

            description = getDescription(book_id);
            content += description;

            nb_corpus = rs.getInt(3);

            Book Abook = new Book(content, book_id, nb_corpus, book_infos);
            Abook.addKeywords(getOnixKeywords(book_id));

            AllBooks.add(Abook);

        }
        rs = null;
        LOG.info("Number of Book To Manage " + AllBooks.size());
        return AllBooks;
    }

    private ArrayList<String[]> getBookInfos(int book_id) throws SQLException {

        String requete = "select a.firstname, a.lastname, b.title from book_author ba, book b, author a WHERE b.book_id = ba.book_id_fk AND ba.author_id_fk = a.author_id AND b.book_id = " + book_id;
        PreparedStatement statement = InsertCon.prepareStatement(requete);
        ResultSet Localrs = statement.executeQuery();
        String firstname, lastname, title;

        ArrayList<String[]> results = new ArrayList<>();

        while (Localrs.next()) {
            firstname = Localrs.getString("firstname");
            lastname = Localrs.getString("lastname");
            title = Localrs.getString("title");
            results.add(new String[]{firstname, lastname, title});
        }

        return results;
    }
    private int onix_keyword_sum = 0;

    private PreparedStatement getFetchKeywordStatement() throws SQLException {
        return InsertCon.prepareStatement("SELECT * FROM keyword k "
                + "INNER JOIN book_keyword bk ON (bk.keyword_id_fk = k.keyword_id) "
                + "INNER JOIN book b ON ( bk.book_id_fk = b.book_id) "
                + "INNER JOIN lemma_type l ON (k.lemma_type_id_fk = l.lemma_type_id) "
                + "WHERE b.book_id = ?");
    }

    KeywordSet getOnixKeywords(int book_id) throws SQLException {

        fetchOnixKeyword.setInt(1, book_id);
        rs = fetchOnixKeyword.executeQuery();

        KeywordSet set = new KeywordSet();
        while (rs.next()) {
            //name , lemma , weight
            set.put(new Keyword(rs.getInt("keyword_id"), rs.getString("k.name"), rs.getString("l.name"), 1.0));
            //name, lemma, global_weight, global_count
            Main.NormierKeywords.put(book_id, new Keyword(rs.getInt("keyword_id"), rs.getString("k.name"), rs.getString("l.name"), rs.getInt("global_weight"), rs.getInt("global_count")));
        }
        onix_keyword_sum += set.size();

        LOG.debug("Querying Database for Additional Onix Keywords for book_id : " + book_id + " Found/Total Currently Found : " + set.size() + "/" + onix_keyword_sum);
        return set;
    }

    private String getDescription(int book_id) throws SQLException {
        String requete = "SELECT `product`.`long_description` FROM `" + DatabaseName + "`.`product` WHERE `product`.`book_id_fk` = '" + book_id + "' LIMIT 1";

        PreparedStatement statement = FetchCon.prepareStatement(requete);
        ResultSet Localrs = statement.executeQuery();

        String description = "";
        while (Localrs.next()) {
            description = Localrs.getString(1);
        }

        return description;
    }

    private int getKeywordId(PreparedStatement s, Keyword pKeyword) throws SQLException {
        if (pKeyword.id != -1) {
            return pKeyword.id;
        }
        s.setString(1, pKeyword.keyword);
        rs = s.executeQuery();

        int kw_id = -1;
        while (rs.next()) {
            kw_id = rs.getInt(1);
        }
        rs = null;

        return kw_id;
    }

    private static String getDate() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new java.util.Date(System.currentTimeMillis()));
    }

    KeywordSet getNormierKeywords() {
        LOG.info("Getting Normier Stored Keywords");
        KeywordSet set = new KeywordSet();
        try {
            String requete = "SELECT * FROM keyword";
            PreparedStatement statement = InsertCon.prepareStatement(requete);
            ResultSet Localrs = statement.executeQuery();

            while (Localrs.next()) {
                try {
                    //name, lemma, global_weight, global_count
                    Main.NormierKeywords.put(new Keyword(Localrs.getInt("keyword_id"),
                            Localrs.getString("name"), Localrs.getString("name"), Localrs.getInt("global_weight"), Localrs.getInt("global_count")));
                } catch (SQLException ex) {
                    LOG.error("Error Fetching", ex);
                }
            }

        } catch (SQLException ex) {
            LOG.error("Error Fetching", ex);
        }
        LOG.info("End Getting Normier Stored Keywords");
        return set;
    }

    public void close() {
        try {
            if (FetchCon != null) {
                FetchCon.close();
            }
            InsertCon.close();
        } catch (SQLException ex) {
            LOG.error("Error Closing Database Connection");
        }
    }
}
