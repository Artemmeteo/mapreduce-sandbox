CREATE TEMP TABLE vacancies AS
SELECT *, list_sort(list_distinct(list_filter(
    list_transform(string_split(coalesce(key_skills, ''), ';'), x -> lower(trim(x))),
    x -> x <> ''
))) AS skills
FROM input_rows
WHERE trim(role) = 'Программист, разработчик'
  AND regexp_matches(lower(name), '(^|[^a-z])java([^a-z]|$)');

CREATE TEMP TABLE skill_rows AS
SELECT source_row, area, skill, pos
FROM vacancies, UNNEST(skills) WITH ORDINALITY AS s(skill, pos);
