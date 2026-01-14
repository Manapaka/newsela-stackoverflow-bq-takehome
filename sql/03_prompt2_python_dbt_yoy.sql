/* ============================================================================
File:        sql/03_prompt2_python_dbt_yoy.sql
Prompt:      2) For posts tagged with ONLY 'python' or ONLY 'dbt':
             - Year-over-year change of question-to-answer ratio for the last 10 calendar years
             - Rate of approved answers for the same period
             - Compare python vs dbt

Dataset:     bigquery-public-data.stackoverflow
Tables:      posts_questions, posts_answers

Key assumptions / definitions:
- "Approved answer" == accepted answer:
    posts_questions.accepted_answer_id IS NOT NULL
- "Question-to-answer ratio" is implemented as answers_per_question:
    total_answers / total_questions
  (average answers per question). If you prefer the inverse, adjust in README or compute both.

Calendar-year requirement:
- This script returns rows for each of the last 10 calendar years relative to CURRENT_DATE()
  even if the dataset has gaps (questions=0 -> metrics NULL).

Outputs:
- Two result sets (run one at a time):
  A) Long format: year x tag with YoY deltas
  B) Comparison format: year with python vs dbt side-by-side + diffs
============================================================================ */

-- -----------------------------
-- PARAMETERS
-- -----------------------------
DECLARE end_year INT64 DEFAULT EXTRACT(YEAR FROM CURRENT_DATE());
-- DECLARE end_year INT64 DEFAULT 2022; -- 2022 year is the last in dataset
DECLARE start_year INT64 DEFAULT EXTRACT(YEAR FROM DATE_SUB(CURRENT_DATE(), INTERVAL 9 YEAR));

WITH
year_spine AS (
  SELECT y AS year
  FROM UNNEST(GENERATE_ARRAY(start_year, end_year)) AS y
),

tags AS (
  SELECT tag FROM UNNEST(['python', 'dbt']) AS tag
),

questions_base AS (
  SELECT
    q.id AS question_id,
    EXTRACT(YEAR FROM q.creation_date) AS year,
    q.accepted_answer_id,
    ARRAY(
      SELECT DISTINCT LOWER(t)
      FROM UNNEST(SPLIT(TRIM(q.tags, '|'), '|')) t
      WHERE t IS NOT NULL AND t != ''
    ) AS tags_arr
  FROM `bigquery-public-data.stackoverflow.posts_questions` q
  WHERE EXTRACT(YEAR FROM q.creation_date) BETWEEN start_year AND end_year
),

-- Only questions with exactly one tag: python OR dbt
questions_filtered AS (
  SELECT
    question_id,
    year,
    (accepted_answer_id IS NOT NULL) AS has_accepted,
    tags_arr[OFFSET(0)] AS only_tag
  FROM questions_base
  WHERE ARRAY_LENGTH(tags_arr) = 1
    AND tags_arr[OFFSET(0)] IN ('python', 'dbt')
),

answers_by_question AS (
  SELECT
    a.parent_id AS question_id,
    COUNT(*) AS answer_cnt
  FROM `bigquery-public-data.stackoverflow.posts_answers` a
  WHERE a.parent_id IN (SELECT question_id FROM questions_filtered)
  GROUP BY 1
),

yearly AS (
  SELECT
    q.only_tag AS tag,
    q.year,
    COUNT(*) AS questions,
    SUM(IFNULL(abq.answer_cnt, 0)) AS answers,
    AVG(IFNULL(abq.answer_cnt, 0)) AS answers_per_question,
    AVG(CASE WHEN q.has_accepted THEN 1 ELSE 0 END) AS accepted_rate
  FROM questions_filtered q
  LEFT JOIN answers_by_question abq
    ON abq.question_id = q.question_id
  GROUP BY 1, 2
),

-- Fill missing calendar years with zeros/NULL metrics
calendar_filled AS (
  SELECT
    t.tag,
    y.year,
    IFNULL(yr.questions, 0) AS questions,
    IFNULL(yr.answers, 0) AS answers,
    CASE WHEN IFNULL(yr.questions, 0) = 0 THEN NULL ELSE yr.answers_per_question END AS answers_per_question,
    CASE WHEN IFNULL(yr.questions, 0) = 0 THEN NULL ELSE yr.accepted_rate END AS accepted_rate
  FROM year_spine y
  CROSS JOIN tags t
  LEFT JOIN yearly yr
    ON yr.year = y.year AND yr.tag = t.tag
),

final_long AS (
  SELECT
    tag,
    year,
    questions,
    answers,
    answers_per_question,
    accepted_rate,
    -- YoY absolute deltas
    answers_per_question
      - LAG(answers_per_question) OVER (PARTITION BY tag ORDER BY year) AS yoy_delta_answers_per_question,
    accepted_rate
      - LAG(accepted_rate) OVER (PARTITION BY tag ORDER BY year) AS yoy_delta_accepted_rate,
    -- YoY relative deltas
    SAFE_DIVIDE(
      answers_per_question - LAG(answers_per_question) OVER (PARTITION BY tag ORDER BY year),
      LAG(answers_per_question) OVER (PARTITION BY tag ORDER BY year)
    ) AS yoy_pct_answers_per_question,
    SAFE_DIVIDE(
      accepted_rate - LAG(accepted_rate) OVER (PARTITION BY tag ORDER BY year),
      LAG(accepted_rate) OVER (PARTITION BY tag ORDER BY year)
    ) AS yoy_pct_accepted_rate
  FROM calendar_filled
),

final_compare AS (
  SELECT
    year,

    MAX(IF(tag = 'python', questions, NULL)) AS python_questions,
    MAX(IF(tag = 'python', answers_per_question, NULL)) AS python_answers_per_question,
    MAX(IF(tag = 'python', accepted_rate, NULL)) AS python_accepted_rate,

    MAX(IF(tag = 'dbt', questions, NULL)) AS dbt_questions,
    MAX(IF(tag = 'dbt', answers_per_question, NULL)) AS dbt_answers_per_question,
    MAX(IF(tag = 'dbt', accepted_rate, NULL)) AS dbt_accepted_rate,

    -- Differences (python - dbt)
    (MAX(IF(tag = 'python', answers_per_question, NULL)) - MAX(IF(tag = 'dbt', answers_per_question, NULL))) AS diff_answers_per_question,
    (MAX(IF(tag = 'python', accepted_rate, NULL)) - MAX(IF(tag = 'dbt', accepted_rate, NULL))) AS diff_accepted_rate
  FROM final_long
  GROUP BY 1
)

-- ============================================================================
-- SELECTS (run one at a time)
-- ============================================================================

-- A) Long format (year x tag)
SELECT *
FROM final_long
ORDER BY year, tag;

-- B) Comparison format (python vs dbt side-by-side + diffs)
-- SELECT *
-- FROM final_compare
-- ORDER BY year;
