/* ============================================================================
File:        sql/07_prompt3_time_of_day.sql
Prompt:      3) Post qualities (non-tag):
             Analyze answered/accepted rates by time of posting:
             - day of week
             - hour of day

Dataset:     bigquery-public-data.stackoverflow
Tables:      posts_questions, posts_answers

Definitions:
- BigQuery DAYOFWEEK: 1=Sunday ... 7=Saturday
- answered_rate = share of questions with >=1 answer
- accepted_rate = share of questions with accepted answer

Calendar window:
- Last 10 calendar years relative to CURRENT_DATE()

Output:
- 7*24 cells (filtered by min volume) with answered_rate and accepted_rate
============================================================================ */

-- -----------------------------
-- PARAMETERS
-- -----------------------------
DECLARE end_year INT64 DEFAULT EXTRACT(YEAR FROM CURRENT_DATE());
-- DECLARE end_year INT64 DEFAULT 2022; -- 2022 year is the last in dataset
DECLARE start_year INT64 DEFAULT EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 9 YEAR));
DECLARE min_questions_per_cell INT64 DEFAULT 200;

WITH
questions AS (
  SELECT
    q.id AS question_id,
    EXTRACT(DAYOFWEEK FROM q.creation_date) AS day_of_week, -- 1=Sun ... 7=Sat
    EXTRACT(HOUR FROM q.creation_date) AS hour_of_day,
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
    q.day_of_week,
    q.hour_of_day,
    q.has_accepted,
    IFNULL(abq.answer_cnt, 0) AS answer_cnt,
    (IFNULL(abq.answer_cnt, 0) > 0) AS answered
  FROM questions q
  LEFT JOIN answers_by_question abq
    ON abq.question_id = q.question_id
)

SELECT
  day_of_week,
  hour_of_day,
  COUNT(*) AS questions,
  AVG(CAST(answered AS INT64)) AS answered_rate,
  AVG(CAST(has_accepted AS INT64)) AS accepted_rate,
  AVG(answer_cnt) AS avg_answers_per_question
FROM feat
GROUP BY 1, 2
HAVING COUNT(*) >= min_questions_per_cell
ORDER BY day_of_week, hour_of_day;
