inputDialogues4 = LOAD 'hdfs:///user/ramanathannatarajan/episodeIV_dialogues.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
inputDialogues5 = LOAD 'hdfs:///user/ramanathannatarajan/episodeV_dialogues.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);
inputDialogues6 = LOAD 'hdfs:///user/ramanathannatarajan/episodeVI_dialogues.txt' USING PigStorage('\t') AS (name:chararray, line:chararray);

-- Add serial numbers to each row and remove the first 2 lines in each file so that only the lines with dialogues remain 
ranked4 = RANK inputDialogues4;
OnlyDialogues4 = FILTER ranked4 BY (rank_inputDialogues4 > 2);
ranked5 = RANK inputDialogues5;
OnlyDialogues5 = FILTER ranked5 BY (rank_inputDialogues5 > 2);
ranked6 = RANK inputDialogues6;
OnlyDialogues6 = FILTER ranked6 BY (rank_inputDialogues6 > 2);

-- Merge the three input files
mergedInputData = UNION OnlyDialogues4, OnlyDialogues5, OnlyDialogues6;

-- Group the dialogues by name column
groupedByNameData = GROUP mergedInputData BY name;

-- Count the number of lines with each name
numberOfNames = FOREACH groupedByNameData GENERATE $0 as name, COUNT($1) AS no_of_lines;
namesOrdered = ORDER numberOfNames BY no_of_lines DESC;

-- Remove the output folder
rmf hdfs:///user/ramanathannatarajan/output;

-- Store result in hdfs
STORE namesOrdered INTO 'hdfs:///user/ramanathannatarajan/outputs' USING PigStorage('\t');
 

