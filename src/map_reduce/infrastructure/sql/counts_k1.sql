SELECT skill AS skill_1, count(*)::BIGINT AS frequency
FROM skill_rows
GROUP BY skill
ORDER BY frequency DESC, skill_1;
