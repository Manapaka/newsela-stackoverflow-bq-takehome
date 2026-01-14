/* ============================================================================
File:        sql/06_prompt3_code_links_buckets.sql
Prompt:      3) Post qualities (non-tag):
             Compare answered/accepted rates by:
             - presence of code block in body
             - link count bucket in body

Dataset:     bigquery-public-data.stackoverflow
Tables:      posts_questions, posts_answers

Definitions:
- has_code_block: body contains ``` or <code>
- link_cnt: number of occurrences of http(s):// in body
- link_bucket: 0, 1, 2-3, 4+
- answered_rate = share of questions with >=1 answer
- accepted_rate = share of questions with accepted answer

Calendar window:
- Last 10 calendar years relative to CURRENT_DATE()

Output:
- Grouped table by has_code_block x link_bucket
============================================================================ */

-- -----------------------------
-- PARAMETERS
-- -----------------------------
DECLARE end_year INT64 DEFAULT EXTRACT(YEAR FROM CURRENT_DATE());
DECLARE start_year INT64 DEFAULT EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 9 YEAR));

WITH
questions AS (
  SELECT
    q.id AS question_id,
    (q.accepted_answer_id IS NOT NULL) AS has_accepted,
    REGEXP_CONTAINS(IFNULL(q.body, ''), r"```|<code>") AS has_code_block,
    ARRAY_LENGTH(REGEXP_EXTRACT_ALL(IFNULL(q.body, ''), r"https?://")) AS link_cnt
  FROM `bigquery-public-data.stackoverflow.posts_questions` q
  WHERE EXTRACT(YEAR FROM q.creation_date) BETWEEN start_year AND end_year
),

answers_by_question AS (
  SELECT
    a.parent_id AS question_id,
    COUNT(*) AS answer_cnt
  FROM `bigquery-public-data.stackoverflow.posts_answers` a
  WHERE a.parent_id IN (SELECT question_id FROM questions)
  GROUP BY 1
),

feat AS (
  SELECT
    q.question_id,
    q.has_accepted,
    q.has_code_block,
    q.link_cnt,
    IFNULL(abq.answer_cnt, 0) AS answer_cnt,
    (IFNULL(abq.answer_cnt, 0) > 0) AS answered
  FROM questions q
  LEFT JOIN answers_by_question abq
    ON abq.question_id = q.question_id
),

grouped AS (
  SELECT
    has_code_block,
    CASE
      WHEN link_cnt = 0 THEN '0'
      WHEN link_cnt = 1 THEN '1'
      WHEN link_cnt BETWEEN 2 AND 3 THEN '2-3'
      ELSE '4+'
    END AS link_bucket,
    COUNT(*) AS questions,
    AVG(CAST(answered AS INT64)) AS answered_rate,
    AVG(CAST(has_accepted AS INT64)) AS accepted_rate,
    AVG(answer_cnt) AS avg_answers_per_question
  FROM feat
  GROUP BY 1, 2
)

SELECT *
FROM grouped
ORDER BY has_code_block DESC,
  CASE link_bucket
    WHEN '0' THEN 1
    WHEN '1' THEN 2
    WHEN '2-3' THEN 3
    ELSE 4
  END;
