/* ============================================================================
File:        sql/05_prompt3_body_len_deciles.sql
Prompt:      3) Post qualities (non-tag):
             Analyze answered/accepted rates by BODY length deciles.

Dataset:     bigquery-public-data.stackoverflow
Tables:      posts_questions, posts_answers

Definitions:
- answered_rate = share of questions with >=1 answer
- accepted_rate = share of questions with accepted answer
- body_len = LENGTH(body) in characters
- Deciles computed via NTILE(10) over body_len

Calendar window:
- Last 10 calendar years relative to CURRENT_DATE()

Output:
- 10 rows (deciles) with rates + avg answers per question
- Includes min/max/median body_len per decile for interpretability
============================================================================ */

-- -----------------------------
-- PARAMETERS
-- -----------------------------
DECLARE end_year INT64 DEFAULT EXTRACT(YEAR FROM CURRENT_DATE());
-- DECLARE end_year INT64 DEFAULT 2022; -- 2022 year is the last in dataset
DECLARE start_year INT64 DEFAULT EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 9 YEAR));

WITH
questions AS (
  SELECT
    q.id AS question_id,
    LENGTH(IFNULL(q.body, '')) AS body_len,
    (q.accepted_answer_id IS NOT NULL) AS has_accepted
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
    q.body_len,
    q.has_accepted,
    IFNULL(abq.answer_cnt, 0) AS answer_cnt,
    (IFNULL(abq.answer_cnt, 0) > 0) AS answered
  FROM questions q
  LEFT JOIN answers_by_question abq
    ON abq.question_id = q.question_id
),

binned AS (
  SELECT
    NTILE(10) OVER (ORDER BY body_len) AS body_len_decile,
    body_len,
    answered,
    has_accepted,
    answer_cnt
  FROM feat
  WHERE body_len IS NOT NULL
)

SELECT
  body_len_decile,
  COUNT(*) AS questions,

  -- decile boundaries to help interpret results
  MIN(body_len) AS min_body_len,
  MAX(body_len) AS max_body_len,
  APPROX_QUANTILES(body_len, 2)[OFFSET(1)] AS median_body_len,

  AVG(CAST(answered AS INT64)) AS answered_rate,
  AVG(CAST(has_accepted AS INT64)) AS accepted_rate,
  AVG(answer_cnt) AS avg_answers_per_question
FROM binned
GROUP BY 1
ORDER BY 1;
