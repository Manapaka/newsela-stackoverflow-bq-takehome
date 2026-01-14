/* ============================================================================
File:        sql/04_prompt3_features_and_corr.sql
Prompt:      3) Other than tags, what qualities on a post correlate with:
             - Highest rate of having at least one answer
             - Highest rate of having an approved (accepted) answer

Dataset:     bigquery-public-data.stackoverflow
Tables:      posts_questions, posts_answers

Key assumptions / definitions:
- "Approved answer" == accepted answer:
    posts_questions.accepted_answer_id IS NOT NULL
- "Answered" == at least one answer exists for the question.
- This is correlation/EDA (not causal). Some metrics (views/score/comments)
  may be influenced by receiving answers (potential reverse causality).

Calendar window:
- Last 10 calendar years relative to CURRENT_DATE()

Outputs:
- One result set: correlations between selected features and outcomes.
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
    q.creation_date,
    EXTRACT(YEAR FROM q.creation_date) AS year,
    EXTRACT(HOUR FROM q.creation_date) AS hour_of_day,
    EXTRACT(DAYOFWEEK FROM q.creation_date) AS day_of_week, -- 1=Sun ... 7=Sat
    q.owner_user_id,

    -- engagement / quality proxies
    q.score,
    q.view_count,
    q.comment_count,
    q.favorite_count,

    q.accepted_answer_id,

    -- content-derived features
    LENGTH(IFNULL(q.title, '')) AS title_len,
    LENGTH(IFNULL(q.body, '')) AS body_len,
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
    q.*,
    IFNULL(abq.answer_cnt, 0) AS answer_cnt,
    (IFNULL(abq.answer_cnt, 0) > 0) AS answered,
    (q.accepted_answer_id IS NOT NULL) AS has_accepted
  FROM questions q
  LEFT JOIN answers_by_question abq
    ON abq.question_id = q.question_id
)

SELECT
  -- Answered correlations
  CORR(CAST(answered AS INT64), view_count) AS corr_answered_view_count,
  CORR(CAST(answered AS INT64), score) AS corr_answered_score,
  CORR(CAST(answered AS INT64), comment_count) AS corr_answered_comment_count,
  CORR(CAST(answered AS INT64), body_len) AS corr_answered_body_len,
  CORR(CAST(answered AS INT64), title_len) AS corr_answered_title_len,

  -- Accepted correlations
  CORR(CAST(has_accepted AS INT64), view_count) AS corr_accepted_view_count,
  CORR(CAST(has_accepted AS INT64), score) AS corr_accepted_score,
  CORR(CAST(has_accepted AS INT64), comment_count) AS corr_accepted_comment_count,
  CORR(CAST(has_accepted AS INT64), body_len) AS corr_accepted_body_len,
  CORR(CAST(has_accepted AS INT64), title_len) AS corr_accepted_title_len
FROM feat
WHERE view_count IS NOT NULL
  AND score IS NOT NULL
  AND comment_count IS NOT NULL
  AND body_len IS NOT NULL
  AND title_len IS NOT NULL;
