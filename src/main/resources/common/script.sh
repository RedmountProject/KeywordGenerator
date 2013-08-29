#!/bin/sh


./CheckDic auteurs.dic --delaf -a Alphabet.txt
./Compress auteurs.dic
./Normalize requete.txt -r Norm.txt
mkdir requete_snt
./Tokenize requete.snt -a Alphabet.txt
./Grf2Fst2 "./sanspapier.grf" -y "--alphabet=Alphabet.txt"
./Locate -t ./requete.snt ./sanspapier.fst2 -a ./Alphabet.txt -L -M -n200 -m dela-fr-public.bin -m auteurs.bin -b -Y
./Concord ./requete_snt/concord.ind "-fCourier new" -s12 -l40 -r55 --html -a Alphabet_sort.txt --CL -t
