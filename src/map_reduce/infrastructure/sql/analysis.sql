CREATE TEMP TABLE analysis_v AS
WITH typed AS (
 SELECT *, try_cast(salary_from AS DOUBLE) AS lo, try_cast(salary_to AS DOUBLE) AS hi
 FROM vacancies
), priced AS (
 SELECT *, CASE WHEN currency IN ('RUR','RUB')
   AND (lo IS NULL OR (lo>0 AND isfinite(lo)))
   AND (hi IS NULL OR (hi>0 AND isfinite(hi)))
   AND (lo IS NULL OR hi IS NULL OR lo<=hi)
 THEN (coalesce(lo,hi)+coalesce(hi,lo))/2 END AS salary_proxy
 FROM typed
)
SELECT *, CASE WHEN lower(gross)='false' THEN salary_proxy END AS salary_net,
          CASE WHEN lower(gross)='true' THEN salary_proxy END AS salary_gross
FROM priced;

CREATE TEMP TABLE combo_rows AS
SELECT 1 AS k, [a.skill] AS stack, a.source_row, a.area, v.experience, v.salary_net
FROM skill_rows a JOIN analysis_v v USING(source_row)
UNION ALL
SELECT 2, [a.skill,b.skill], a.source_row, a.area, v.experience, v.salary_net
FROM skill_rows a JOIN skill_rows b ON a.source_row=b.source_row AND a.pos<b.pos
JOIN analysis_v v ON a.source_row=v.source_row
UNION ALL
SELECT 3, [a.skill,b.skill,c.skill], a.source_row, a.area, v.experience, v.salary_net
FROM skill_rows a JOIN skill_rows b ON a.source_row=b.source_row AND a.pos<b.pos
JOIN skill_rows c ON a.source_row=c.source_row AND b.pos<c.pos
JOIN analysis_v v ON a.source_row=v.source_row;

CREATE TEMP TABLE combo_stats AS
SELECT k, stack, count(*) AS vacancies, count(salary_net) AS salary_n,
       median(salary_net) AS median_net_rub
FROM combo_rows GROUP BY k,stack;

CREATE TEMP TABLE chosen_bases AS
WITH candidates AS (
 SELECT *, unnest(CASE WHEN k=2 THEN list_transform(stack,x -> [x])
              ELSE [[stack[1],stack[2]],[stack[1],stack[3]],[stack[2],stack[3]]] END) AS candidate_base
 FROM combo_stats WHERE k IN (2,3)
)
SELECT s.k, s.stack, s.vacancies, s.salary_n, s.median_net_rub,
       b.stack AS base_stack, b.vacancies AS base_vacancies,
       b.salary_n AS base_salary_n, b.median_net_rub AS base_median_net_rub
FROM candidates s JOIN combo_stats b
  ON b.k=s.k-1 AND b.stack=s.candidate_base
QUALIFY row_number() OVER(PARTITION BY s.k,s.stack ORDER BY b.vacancies DESC,b.stack)=1;

CREATE TEMP TABLE salary_effects AS
SELECT c.*, list_filter(c.stack,x -> NOT list_contains(c.base_stack,x)) AS added_skills,
       c.median_net_rub-c.base_median_net_rub AS delta_rub,
       100*(c.median_net_rub/c.base_median_net_rub-1) AS delta_pct,
       count(r.salary_net) FILTER(WHERE NOT list_has_all(v.skills,c.stack)) AS base_without_added_n,
       median(r.salary_net) FILTER(WHERE NOT list_has_all(v.skills,c.stack)) AS base_without_added_median,
       c.median_net_rub - median(r.salary_net) FILTER(WHERE NOT list_has_all(v.skills,c.stack))
         AS exclusive_delta_rub
FROM chosen_bases c JOIN combo_rows r ON r.k=c.k-1 AND r.stack=c.base_stack
JOIN analysis_v v ON v.source_row=r.source_row
GROUP BY c.k,c.stack,c.vacancies,c.salary_n,c.median_net_rub,c.base_stack,
         c.base_vacancies,c.base_salary_n,c.base_median_net_rub;
