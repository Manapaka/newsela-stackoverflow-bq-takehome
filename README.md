# newsela-stackoverflow-bq-takehome
Newsela Senior Analytics Engineering take-home: Stack Overflow BigQuery analysis

## Common definitions and assumptions

### Approved answers
“Approved answer” is interpreted as an **accepted answer**:

- `posts_questions.accepted_answer_id IS NOT NULL`

### Answer rate metrics
At the question level:
- `answered` = question has at least one answer
- `has_accepted` = question has an accepted answer
- `answer_cnt` = number of answers

At aggregated level:
- `answered_rate` = `AVG(answered)`
- `accepted_rate` = `AVG(has_accepted)`

### “Question-to-answer ratio” (Prompt 2)
Implemented as:

- `answers_per_question = total_answers / total_questions`

---

# Prompt 1 — Tags and tag combinations (current calendar year)

## Queries
- `sql/01_prompt1_single_tags.sql`
- `sql/02_prompt1_tag_pairs.sql`

## Approach
1. Filter questions to the **current calendar year**.
2. Parse tags into a normalized array.
3. Compute per-tag (and per-pair) aggregates:
   - question count
   - total answers
   - answers per question
   - answered rate
   - accepted (approved) rate
4. Return:
   - Top and bottom tags/pairs by `answers_per_question`
   - Top and bottom tags/pairs by `accepted_rate`

---

# Prompt 2 — `python` vs `dbt` (only-tag questions), YoY over last 10 calendar years

## Query
- `sql/03_prompt2_python_dbt_yoy.sql`

## Approach
1. Build a **calendar year spine** for the last 10 calendar years (relative to `CURRENT_DATE()`).
2. Filter to questions that have **exactly one tag**, and that tag is `python` or `dbt`.
3. Join in answer counts (`posts_answers.parent_id = question_id`).
4. Compute per year per tag:
   - `answers_per_question`
   - `accepted_rate`
5. Compute YoY deltas and percent changes using window functions.
6. Provide both:
   - “Long” format (year × tag)
   - “Compare” format (side-by-side python vs dbt, plus diffs)

## Observations (from an example run)
Because the dataset snapshot appears to end around 2022, years beyond coverage show 0 questions and `NULL` rates.

Within the covered period:
- `python` has high volume (e.g., ~11k–16k “only python” questions/year in 2017–2022 in the sample run).
- `dbt` appears later and is low volume in early years (e.g., tens of questions/year in 2020–2022 in the sample run), which makes its year-over-year metrics noisier.

Trend examples observed in the sample run:
- `python`:
  - `answers_per_question` decreases from ~1.70 (2017–2019) to ~1.26 (2022).
  - `accepted_rate` decreases from ~0.49 (2017–2018) to ~0.35 (2022).
- `dbt` (coverage begins later in the snapshot):
  - `answers_per_question` around ~1.39 (2020) declining toward ~1.06 (2022).
  - `accepted_rate` in the ~0.26–0.42 range in 2020–2022 (volatile due to low volume).

---

# Prompt 3 — Non-tag post qualities correlated with answer and accepted-answer rates

## Queries
- `sql/04_prompt3_features_and_corr.sql` (correlations)
- `sql/05_prompt3_body_len_deciles.sql` (body length deciles)
- `sql/06_prompt3_code_links_buckets.sql` (code blocks × link buckets)
- `sql/07_prompt3_time_of_day.sql` (day-of-week × hour-of-day)

### Key observations (from example runs)

#### A) Correlation screen (indicative)
Example correlation directions observed:
- Small positive correlations:
  - `answered` with `view_count` (~0.065)
  - `accepted` with `score` (~0.062)
- Negative correlation:
  - `answered` with `comment_count` (~-0.15), suggesting comment-heavy questions may be harder to resolve quickly.

**Interpretation caveat:**  
`view_count`, `score`, and `comment_count` may be affected by whether a question receives answers (reverse causality). Therefore, the bucket analyses below are more actionable and interpretable.

#### B) Body length deciles (content depth)
From the decile analysis:
- `answered_rate` is highest in shorter/mid body lengths (~0.80+) and declines for the longest decile (~0.74).
- `accepted_rate` peaks in mid deciles (~0.46 around deciles 6–7), and is lower for the shortest decile (~0.37) and the longest decile (~0.41).

**Takeaway:**  
Moderately detailed questions tend to have the best accepted-answer rates; extremely long questions correlate with lower answer and accepted rates.

#### C) Code blocks + links (structure / reproducibility)
This was the strongest signal in the sample run:

- With a code block:
  - `accepted_rate` ~0.45–0.46 (across link buckets)
  - `answered_rate` ~0.77–0.81
- Without a code block:
  - `accepted_rate` ~0.31–0.35
  - `answered_rate` ~0.72–0.74

**Takeaway:**  
Presence of a code block is strongly associated with a higher likelihood of receiving answers and an accepted answer.
