data = 
    LOAD 'hotel-review.csv' 
    USING PigStorage(';') 
    AS (id:chararray, review:chararray, aspect:chararray, category:chararray, sentiment:chararray);

stopwords = LOAD 'stopwords.txt' USING PigStorage(';') AS (word:chararray);

data = FILTER data BY id != 'id' AND 
       (sentiment == 'positive' OR sentiment == 'negative' OR sentiment == 'neutral');

-----BÀI 1
-- Đưa về chữ thường
data = FOREACH data GENERATE 
    (int)id AS id,
    LOWER(REPLACE(review, '[?!;,.]', '')) AS review, 
    aspect, 
    category, 
    sentiment;

-- Tách thành các từ
words = FOREACH data GENERATE 
    id,
    FLATTEN(TOKENIZE(review)) AS word, 
    aspect, 
    category, 
    sentiment;

-- Loại bỏ stopword
words_join = JOIN words BY word LEFT OUTER, stopwords BY word USING 'replicated';
final_words = FILTER words_join BY stopwords::word IS NULL;
clean_data = FOREACH final_words GENERATE 
    words::id AS id, 
    words::word AS word,
    words::aspect AS aspect,
    words::category AS category,
    words::sentiment AS sentiment;

-- samples = LIMIT clean_data 10;
-- DUMP samples;
-- STORE clean_data INTO 'Bai1_output' USING PigStorage(';');

-----BÀI 2
-- Top 5 từ xuất hiện nhiều nhất
words = GROUP clean_data BY word;
word_counts = FOREACH words GENERATE 
    group AS word, 
    COUNT(clean_data) AS frequency;
all_words = GROUP word_counts ALL;
top5 = FOREACH all_words GENERATE TOP(5, 1, word_counts) AS top_words;
top5_words = FOREACH top5 GENERATE FLATTEN(top_words) AS (word:chararray, frequency:long);

-- DUMP top5_words;
-- STORE top5_words INTO 'Bai2_output/Top5Words' USING PigStorage(';');

-- Thống kê theo Category
category_groups = GROUP data BY category;
category_counts = FOREACH category_groups GENERATE 
    group AS category, 
    COUNT(data) AS num_comments;

-- DUMP category_counts;
-- STORE category_counts INTO 'Bai2_output/Category' USING PigStorage(';');

-- Thống kê theo Aspect
aspect_groups = GROUP data BY aspect;
aspect_counts = FOREACH aspect_groups GENERATE 
    group AS aspect, 
    COUNT(data) AS num_comments;

-- DUMP aspect_counts;
-- STORE aspect_counts INTO 'Bai2_output/Aspect' USING PigStorage(';');

-----BÀI 3
-- Lọc ra các dòng có sentiment là 'negative'
neg_data = FILTER data BY sentiment == 'negative';
neg_grouped = GROUP neg_data BY (aspect, sentiment);
neg_counts = FOREACH neg_grouped GENERATE 
    group.aspect AS aspect, 
    group.sentiment AS sentiment,
    COUNT(neg_data) AS frequency;
neg_all = GROUP neg_counts ALL;
top1_neg = FOREACH neg_all GENERATE TOP(1, 2, neg_counts) AS top_aspect;
top1_neg_aspect = FOREACH top1_neg GENERATE FLATTEN(top_aspect) AS (aspect:chararray, sentiment:chararray, frequency:long);

-- DUMP top1_neg_aspect;
-- STORE top1_neg_aspect INTO 'Bai3_output/Most_Negative_Aspect' USING PigStorage(';');

-- Lọc ra các dòng có sentiment là 'positive'
pos_data = FILTER data BY sentiment == 'positive';
pos_grouped = GROUP pos_data BY (aspect, sentiment);
pos_counts = FOREACH pos_grouped GENERATE 
    group.aspect AS aspect, 
    group.sentiment AS sentiment,
    COUNT(pos_data) AS frequency;
pos_all = GROUP pos_counts ALL;
top1_pos = FOREACH pos_all GENERATE TOP(1, 2, pos_counts) AS top_aspect;
top1_pos_aspect = FOREACH top1_pos GENERATE FLATTEN(top_aspect) AS (aspect:chararray, sentiment:chararray, frequency:long);

-- DUMP top1_pos_aspect;
-- STORE top1_pos_aspect INTO 'Bai3_output/Most_Positive_Aspect' USING PigStorage(';');

-----BÀI 4
-- Top 5 từ tích cực nhất theo Category
pos_words = FILTER clean_data BY sentiment == 'positive';
pos_word_grouped = GROUP pos_words BY (category, word);
pos_word_counts = FOREACH pos_word_grouped GENERATE 
    group.category AS category, 
    group.word AS word, 
    COUNT(pos_words) AS frequency;
pos_cat_grouped = GROUP pos_word_counts BY category;
top5_pos = FOREACH pos_cat_grouped GENERATE TOP(5, 2, pos_word_counts) AS top_words; 
top5_pos_cat = FOREACH top5_pos GENERATE FLATTEN(top_words) AS (category:chararray, word:chararray, frequency:long);

-- DUMP top5_pos_cat;
-- STORE top5_pos_cat INTO 'Bai4_output/Top5_Positive' USING PigStorage(';');

-- Top 5 từ tiêu cực nhất theo Category
neg_words = FILTER clean_data BY sentiment == 'negative';
neg_word_grouped = GROUP neg_words BY (category, word);
neg_word_counts = FOREACH neg_word_grouped GENERATE 
    group.category AS category, 
    group.word AS word, 
    COUNT(neg_words) AS frequency;
neg_cat_grouped = GROUP neg_word_counts BY category;
top5_neg = FOREACH neg_cat_grouped GENERATE TOP(5, 2, neg_word_counts) AS top_words;
top5_neg_cat = FOREACH top5_neg GENERATE FLATTEN(top_words) AS (category:chararray, word:chararray, frequency:long);

-- DUMP top5_neg_cat;
-- STORE top5_neg_cat INTO 'Bai4_output/Top5_Negative' USING PigStorage(';');

-----BÀI 5
-- Nhóm dữ liệu theo category và word để đếm số lần xuất hiện của mỗi từ trong từng category
cat_word_grouped = GROUP clean_data BY (category, word);
cat_word_counts = FOREACH cat_word_grouped GENERATE 
    group.category AS category, 
    group.word AS word, 
    COUNT(clean_data) AS frequency;
cat_grouped = GROUP cat_word_counts BY category;
top5_cat = FOREACH cat_grouped GENERATE TOP(5, 2, cat_word_counts) AS top_words;
top5_relevant_words = FOREACH top5_cat GENERATE FLATTEN(top_words) AS (category:chararray, word:chararray, frequency:long);

DUMP top5_relevant_words;
STORE top5_relevant_words INTO 'Bai5_output' USING PigStorage(';');