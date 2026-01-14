/* ============================================================================
File:        sql/02_prompt1_tag_pairs.sql
Prompt:      1) Tag combinations (pairs) for the current calendar year:
             - Which tag pairs lead to the most answers?
             - Which tag pairs have the highest rate of approved answers?
             - Which lead to the least?
Dataset:     bigquery-public-data.stackoverflow
Tables:      posts_questions, posts_answers

Key assumptions / definitions:
- "Approved answer" == accepted answer:
    posts_questions.accepted_answer_id IS NOT NULL
- "Most answers" measured as answers_per_question = total_answers / questions
  at the pair level (questions containing BOTH tags in the pair).
- Pairs are UNORDERED: (a,b) == (b,a).

Cost / performance notes:
- Filter to current calendar year early.
- Aggregate answers only for the filtered question ids.
- Generate pairs within each question using UNNEST with offsets (o2 > o1).
- Limit max number of tags per question to prevent combinatorial explosion.
- Require a minimum number of questions per pair to reduce noise.

Outputs:
- Multiple result sets (run one SELECT at a time):
  A) Top pairs by answers_per_question
  B) Bottom pairs by answers_per_question
  C) Top pairs by accepted_rate
  D) Bottom pairs by accepted_rate
============================================================================ */

-- -----------------------------
-- PARAMETERS (tune as needed)
-- -----------------------------
DECLARE target_year INT64 DEFAULT EXTRACT(YEAR FROM CURRENT_DATE());
DECLARE min_questions_per_pair INT64 DEFAULT 200;  -- increase to reduce noise
DECLARE max_tags_per_question INT64 DEFAULT 5;     -- cost guardrail: only build pairs when <= N tags

-- -----------------------------
-- BASE CTEs
-- -----------------------------
WITH
questions AS (
  SELECT
    q.id AS question_id,
    q.accepted_answer_id,
    ARRAY(
      SELECT DISTINCT LOWER(t)
      FROM UNNEST(SPLIT(TRIM(q.tags, '|'), '|')) AS t
      WHERE t IS NOT NULL AND t != ''
    ) AS tags_arr
  FROM `bigquery-public-data.stackoverflow.posts_questions` q
  WHERE EXTRACT(YEAR FROM q.creation_date) = target_year
),

-- Optional cost guardrail: skip questions with too many tags (pair explosion)
questions_limited AS (
  SELECT *
  FROM questions
  WHERE ARRAY_LENGTH(tags_arr) BETWEEN 2 AND max_tags_per_question
),

answers_by_question AS (
  SELECT
    a.parent_id AS question_id,
    COUNT(*) AS answer_cnt
  FROM `bigquery-public-data.stackoverflow.posts_answers` a
  WHERE a.parent_id IN (SELECT question_id FROM questions_limited)
  GROUP BY 1
),

q_enriched AS (
  SELECT
    q.question_id,
    IFNULL(abq.answer_cnt, 0) AS answer_cnt,
    (q.accepted_answer_id IS NOT NULL) AS has_accepted,
    q.tags_arr
  FROM questions_limited q
  LEFT JOIN answers_by_question abq
    ON abq.question_id = q.question_id
),

pairs AS (
  SELECT
    qe.question_id,
    qe.answer_cnt,
    qe.has_accepted,
    t1 AS tag1,
    t2 AS tag2
  FROM q_enriched qe
  CROSS JOIN UNNEST(qe.tags_arr) AS t1 WITH OFFSET o1
  CROSS JOIN UNNEST(qe.tags_arr) AS t2 WITH OFFSET o2
  WHERE o2 > o1  -- ensures unordered unique pairs and avoids (tag, tag)
),

pair_stats AS (
  SELECT
    tag1,
    tag2,
    COUNT(*) AS questions,
    SUM(answer_cnt) AS total_answers,
    AVG(answer_cnt) AS answers_per_question,
    AVG(CASE WHEN answer_cnt > 0 THEN 1 ELSE 0 END) AS answered_rate,
    AVG(CASE WHEN has_accepted THEN 1 ELSE 0 END) AS accepted_rate
  FROM pairs
  GROUP BY 1, 2
  HAVING questions >= min_questions_per_pair
)

-- ============================================================================
-- SELECTS (run one at a time)
-- ============================================================================

-- A) TOP pairs by answers_per_question
SELECT
  CONCAT(tag1, ' + ', tag2) AS tag_pair,
  questions,
  total_answers,
  answers_per_question,
  answered_rate,
  accepted_rate
FROM pair_stats
ORDER BY answers_per_question DESC, questions DESC
LIMIT 100;

-- B) BOTTOM pairs by answers_per_question
-- SELECT
--   CONCAT(tag1, ' + ', tag2) AS tag_pair,
--   questions,
--   total_answers,
--   answers_per_question,
--   answered_rate,
--   accepted_rate
-- FROM pair_stats
-- ORDER BY answers_per_question ASC, questions DESC
-- LIMIT 100;

-- C) TOP pairs by accepted_rate
-- SELECT
--   CONCAT(tag1, ' + ', tag2) AS tag_pair,
--   questions,
--   total_answers,
--   answers_per_question,
--   answered_rate,
--   accepted_rate
-- FROM pair_stats
-- ORDER BY accepted_rate DESC, questions DESC
-- LIMIT 100;

-- D) BOTTOM pairs by accepted_rate
-- SELECT
--   CONCAT(tag1, ' + ', tag2) AS tag_pair,
--   questions,
--   total_answers,
--   answers_per_question,
--   answered_rate,
--   accepted_rate
-- FROM pair_stats
-- ORDER BY accepted_rate ASC, questions DESC
-- LIMIT 100;
