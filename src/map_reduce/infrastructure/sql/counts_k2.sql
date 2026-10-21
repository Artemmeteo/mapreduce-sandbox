SELECT a.skill AS skill_1, b.skill AS skill_2, count(*)::BIGINT AS frequency
FROM skill_rows a JOIN skill_rows b
  ON a.source_row=b.source_row AND a.pos<b.pos
GROUP BY a.skill, b.skill
ORDER BY frequency DESC, skill_1, skill_2;
