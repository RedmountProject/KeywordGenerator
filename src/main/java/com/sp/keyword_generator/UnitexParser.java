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

import fr.umlv.unitex.jni.UnitexJni;
import java.io.UnsupportedEncodingException;
import java.util.*;

import org.apache.commons.lang.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class UnitexParser {

    public static final Logger LOG = LoggerFactory.getLogger(UnitexParser.class.getName());
    private String PFX = "$:";
    static String path;
    ArrayList<String> Keywords = new ArrayList<>();
    private final Config conf;
    private static String sample_path;
    

    public UnitexParser(Config conf) {
        this.conf = conf;
        sample_path = conf.getProperty("sample.expr.path");
        path = conf.getProperty("pwd") + "/src/main/resources/";
        loadDictionariesAndAlphabets();
    }

    private static void loadDictionariesAndAlphabets() {

        String[] dictionaries = {"dic/dela-fr-public.bin", "dic/auteurs.bin", "dic/stopwords.bin"};

        for (String dic : dictionaries) {
            UnitexJni.loadPersistentDictionary(path + dic);
            LOG.info( "Loading : "+path+dic);
        }
        
        UnitexJni.loadPersistentDictionary(path+"dic/sample-dlcf.bin");

        String[] graphs = {"graph/sanspapier.fst2", "graph/cdic.fst2"};

        for (String graph : graphs) {
            UnitexJni.loadPersistentFst2(path + graph);
            LOG.info( "Loading : "+path+graph);
        }

        String[] alphabets = {"common/Alphabet.txt", "common/Alphabet_sort.txt", "common/Norm.txt"};

        for (String alphabet : alphabets) {
            UnitexJni.loadPersistentAlphabet(path + alphabet);
            LOG.info( "Loading : "+path+alphabet);
        }
        LOG.info( "Unitex Graphs And Dictionnaries Loaded");
    }

    public void AddKeywords(Book ABook) throws UnsupportedEncodingException {
        String UnParsedKeywords;
        String data = ABook.content;
        UnParsedKeywords = getUnitexKeyword(data);

        KeywordSet ParsedKeywords = parseKeywords(UnParsedKeywords, ABook);

        ABook.addKeywords(ParsedKeywords);


    }


    public String getUnitexKeyword(String input) throws UnsupportedEncodingException {
        
        String temp = PFX + path + "temp.txt";
        String tempsnt = PFX + path + "temp.snt";

        UnitexJni.setStdOutTrashMode(true);
        UnitexJni.setStdErrTrashMode(true);

        
        if (!UnitexJni.writeUnitexFileUtf(temp, input, false)) {
            LOG.info("Error virtualizing query string");
            return null;
        }

        if (UnitexJni.execUnitexTool(new String[]{"UnitexToolLogger", "Normalize", temp,
                    "-r" + PFX + path + "common/Norm.txt"}) != 0) {
            LOG.info("Error in Normalize");
            return null;
        }

        if (UnitexJni.execUnitexTool(new String[]{"UnitexToolLogger", "Tokenize", tempsnt,
                    "-a" + PFX + path + "common/Alphabet.txt"}) != 0) {
            LOG.info("Error in Tokenize");
            return null;
        }
        if (UnitexJni.execUnitexTool(new String[]{"UnitexToolLogger", "Dico", "-t"
                    + tempsnt,
                    "-a" + PFX + path + "common/Alphabet.txt",
                    PFX + path + "dic/sample-dlcf.bin",
                    PFX + path + "dic/stopwords.bin",
                    PFX + path + "dic/dela-fr-public.bin"}) != 0) {
            LOG.info("Error in Dico");
            return null;
        }
        if (UnitexJni.execUnitexTool(new String[]{"UnitexToolLogger", "Locate", "-t"
                    + tempsnt,
                    "" + PFX + path + "graph/cdic.fst2", "-A", "-I", "--all",
                    "-m" + PFX + path + "dic/sample-dlcf.bin",
                    "-m" + PFX + path + "dic/dela-fr-public.bin",
                    "-m" + PFX + path + "dic/stopwords.bin",
                    "-m" + PFX + path + "dic/auteurs.bin",
                    "-m" + PFX + path + "dic/dela-fr-public.bin", "-b", "-Y", "-qutf8-no-bom"}) != 0) {
            LOG.info("Error in Locate");
            return null;
        }
        if (UnitexJni.execUnitexTool(new String[]{"UnitexToolLogger", "Concord", ""
                    + PFX + path + "temp_snt/concord.ind", "--text",
                    "-a" + PFX + path + "common/Alphabet_sort.txt", "--only_matches", "-qutf8-no-bom", "--CL"}) != 0) {
            LOG.info("Error in Concord");
            return null;
        }
        if (UnitexJni.execUnitexTool(new String[]{"UnitexToolLogger", "SortTxt", "" + PFX + path + "temp_snt/dlf"}) != 0) {
            LOG.info("Error in SortTxt");
            return null;
        }
        if (UnitexJni.execUnitexTool(new String[]{"UnitexToolLogger", "SortTxt", "" + PFX + path + "temp_snt/dlc"}) != 0) {
            LOG.info("Error in SortTxt");
            return null;
        }
        if (UnitexJni.execUnitexTool(new String[]{"UnitexToolLogger", "KeyWords", "" + PFX + path + "temp_snt/tok_by_freq.txt", "-c" + PFX + path + "temp_snt/concord.txt", PFX + path + "temp_snt/dlf", PFX + path + "temp_snt/dlc", "-o" + PFX + path + "out.txt", "-qutf8-no-bom"}) != 0) {
            LOG.info("Error in Keywords");
            return null;
        }

        byte[] s;

        if ((s = UnitexJni.getUnitexFileData(PFX + path + "out.txt")) == null) {
            LOG.info("Error in getUnitexFileString temp_snt/concord.txt");
            return null;
        }
        
        return new String(s, "UTF-8");
    }

    public KeywordSet parseKeywords(String s, Book ABook) {

        boolean Normier = Boolean.parseBoolean(conf.getProperty("advance.keyword.generation"));

        String line;

        
        if (s.compareTo("") == 0) {
            return new KeywordSet();
        }
        StringTokenizer st = new StringTokenizer(s, "\n");
        KeywordSet KeywordList = new KeywordSet();

        for (int i = 0; i < 59 && st.hasMoreElements(); i++, st.hasMoreTokens()) {

            try {
                line = st.nextToken();
            } catch (java.util.NoSuchElementException e) {
                LOG.info("No more elements");
                break;
            }

            String[] temp = line.split("\t");
            String keywordLemma = StringEscapeUtils.escapeSql(temp[1].trim());

            String weight = temp[0].trim();

            String[] ArrayStopWords = {"père", "livre", "dit", "ISBN", "isbn", "mère", "fille", "fils", "famille", "homme", "femme",
                "histoire", "an", "année", "premier", "dernier", "histoire", "vie", "jours", "fin", "jeune", "vieux", "bon",
                "critique.N", "mai.N", "critiques.N", "critiques.N", "page.N", "pages.N", "critique.A", "critiques.A",
                "maison d'édition", "tome", "commentaire", "grand", "petit", "premier", "nouveau",
                "certain", "édition", "tome", "commentaire", "éditeur", "babelio", "LiliGalipette", "chose",
                "choses", "titre", "livre de poche", "BVIALLET", "Vienlivre", "nom", "lecture",
                "mis", "brigetoun", "brigittelascombe", "Evene", "null", "vincentf", "première", "br", "différent", "monde",
                "différents", "http", "Livréà"};

            ArrayList<String> Stopwords = new ArrayList<>(Arrays.asList(ArrayStopWords));

            temp = keywordLemma.split("\\.");
            String lemma = null;

            if (temp.length > 1) {
                lemma = temp[1];
            }

            String keyword = temp[0];

            if ("N".equals(lemma) || "A".equals(lemma) || (keyword.matches("^[A-Z].*") && "".equals(lemma))) {

                if (/*!Stopwords.contains(keywordLemma) && !Stopwords.contains(keyword) && */keyword != null && keyword.length() > 1) {
                    if (lemma.isEmpty() || "".equals(lemma)) {
                        lemma = "NP";
                    }

                    Keyword new_kw = new Keyword(keyword, lemma, Double.parseDouble(weight));

                    if (Normier) {
                        Main.NormierKeywords.put(new_kw);
                    }

                    KeywordList.put(new_kw);
                }
            }
        }
        return KeywordList;
    }

    public static void sleep(int time) {
        try {
            Thread.currentThread().sleep(time);
        } catch (InterruptedException ie) {
        }
    }

    public void print(String s) {
        System.out.println(s);
        sleep(300);
    }
}
