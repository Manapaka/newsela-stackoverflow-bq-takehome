/* ============================================================================
File:        sql/01_prompt1_single_tags.sql
Prompt:      1) Single-tag analysis for the current calendar year:
             - Which tags lead to the most answers?
             - Which tags have the highest rate of approved answers?
             - Which tags lead to the least?
Dataset:     bigquery-public-data.stackoverflow
Tables:      posts_questions, posts_answers

Key assumptions / definitions:
- "Approved answer" is interpreted as an accepted answer:
    posts_questions.accepted_answer_id IS NOT NULL
- "Most answers" is measured as answers_per_question = total_answers / questions
  (i.e., average number of answers per question, for questions tagged with the tag)

Cost / performance notes:
- Filter questions to current calendar year early.
- Aggregate answers only for the filtered question ids.
- Use a minimum question threshold per tag to reduce noise.

Outputs:
- Multiple result sets (run one SELECT at a time depending on what you want):
  A) Top tags by answers_per_question
  B) Bottom tags by answers_per_question
  C) Top tags by accepted_rate
  D) Bottom tags by accepted_rate
============================================================================ */

-- -----------------------------
-- PARAMETERS (tune as needed)
-- -----------------------------
DECLARE min_questions_per_tag INT64 DEFAULT 200;  -- raise to reduce noise, lower to include more tags
DECLARE target_year INT64 DEFAULT EXTRACT(YEAR FROM CURRENT_DATE());
-- DECLARE target_year INT64 DEFAULT 2022; -- 2022 year is the last in dataset

-- -----------------------------
-- BASE CTEs
-- -----------------------------
WITH
questions AS (
  SELECT
    q.id AS question_id,
    q.accepted_answer_id,
    q.creation_date,
    -- Normalize tags: '|python|pandas|' -> ['python','pandas']
    ARRAY(
      SELECT DISTINCT LOWER(t)
      FROM UNNEST(SPLIT(TRIM(q.tags, '|'), '|')) AS t
      WHERE t IS NOT NULL AND t != ''
    ) AS tags_arr
  FROM `bigquery-public-data.stackoverflow.posts_questions` q
  WHERE EXTRACT(YEAR FROM q.creation_date) = target_year
),

answers_by_question AS (
  SELECT
    a.parent_id AS question_id,
    COUNT(*) AS answer_cnt
  FROM `bigquery-public-data.stackoverflow.posts_answers` a
  WHERE a.parent_id IN (SELECT question_id FROM questions)
  GROUP BY 1
),

q_enriched AS (
  SELECT
    q.question_id,
    IFNULL(abq.answer_cnt, 0) AS answer_cnt,
    (q.accepted_answer_id IS NOT NULL) AS has_accepted,
    q.tags_arr
  FROM questions q
  LEFT JOIN answers_by_question abq
    ON abq.question_id = q.question_id
),

tag_stats AS (
  SELECT
    tag,
    COUNT(*) AS questions,
    SUM(answer_cnt) AS total_answers,
    AVG(answer_cnt) AS answers_per_question,
    AVG(CASE WHEN answer_cnt > 0 THEN 1 ELSE 0 END) AS answered_rate,
    AVG(CASE WHEN has_accepted THEN 1 ELSE 0 END) AS accepted_rate
  FROM q_enriched, UNNEST(tags_arr) AS tag
  GROUP BY 1
  HAVING questions >= min_questions_per_tag
)

-- ============================================================================
-- SELECTS (run one at a time)
-- ============================================================================

-- A) TOP tags by answers_per_question
SELECT
  tag,
  questions,
  total_answers,
  answers_per_question,
  answered_rate,
  accepted_rate
FROM tag_stats
ORDER BY answers_per_question DESC, questions DESC
LIMIT 100;

-- B) BOTTOM tags by answers_per_question
-- SELECT
--   tag,
--   questions,
--   total_answers,
--   answers_per_question,
--   answered_rate,
--   accepted_rate
-- FROM tag_stats
-- ORDER BY answers_per_question ASC, questions DESC
-- LIMIT 100;

-- C) TOP tags by accepted_rate
-- SELECT
--   tag,
--   questions,
--   total_answers,
--   answers_per_question,
--   answered_rate,
--   accepted_rate
-- FROM tag_stats
-- ORDER BY accepted_rate DESC, questions DESC
-- LIMIT 100;

-- D) BOTTOM tags by accepted_rate
-- SELECT
--   tag,
--   questions,
--   total_answers,
--   answers_per_question,
--   answered_rate,
--   accepted_rate
-- FROM tag_stats
-- ORDER BY accepted_rate ASC, questions DESC
-- LIMIT 100;

