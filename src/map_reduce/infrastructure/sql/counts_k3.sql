SELECT a.skill AS skill_1, b.skill AS skill_2, c.skill AS skill_3,
       count(*)::BIGINT AS frequency
FROM skill_rows a JOIN skill_rows b
  ON a.source_row=b.source_row AND a.pos<b.pos
JOIN skill_rows c ON a.source_row=c.source_row AND b.pos<c.pos
GROUP BY a.skill, b.skill, c.skill
ORDER BY frequency DESC, skill_1, skill_2, skill_3;
