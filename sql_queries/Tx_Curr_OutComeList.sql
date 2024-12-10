WITH FollowUp AS (SELECT follow_up.encounter_id,
                         follow_up.client_id,
                         follow_up_date_followup_      AS follow_up_date,
                         follow_up_status,
                         art_antiretroviral_start_date AS art_start_date,
                         treatment_end_date            AS art_dose_end,
                         next_visit_date,
                         age
                  FROM mamba_flat_encounter_follow_up follow_up
                           JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                ON follow_up.encounter_id = follow_up_1.encounter_id
                           JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                ON follow_up.encounter_id = follow_up_2.encounter_id
                            JOIN mamba_dim_person person on follow_up.client_id=person.person_id),

-- Consolidated temp CTE for row number calculation
     temp_latest AS (SELECT encounter_id,
                            client_id,
                            follow_up_date                                                                             AS FollowupDate,
                            ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                     FROM FollowUp
                     WHERE follow_up_status IS NOT NULL
                       AND art_start_date IS NOT NULL
                       AND follow_up_date <= REPORT_END_DATE),

-- Select the latest follow-up per client
     latest_follow_up AS (SELECT *
                          FROM temp_latest
                          WHERE row_num = 1),


-- Consolidated temp CTE for row number calculation
     temp_previous AS (SELECT encounter_id,
                              client_id,
                              follow_up_date                                                                             AS FollowupDate,
                              ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                       FROM FollowUp
                       WHERE follow_up_status IS NOT NULL
                         AND art_start_date IS NOT NULL
                         AND follow_up_date <= REPORT_START_DATE),

-- Select the latest follow-up per client
     previous_follow_up AS (SELECT *
                            FROM temp_previous
                            WHERE row_num = 1),
-- TO BE ADDED
     to_be_added AS (select SUM(IF(TI = 'NTI' AND New = 'E' AND follow_up_status = 5, total, 0)) AS Traced_Back,
                            SUM(IF(TI = 'NTI' AND New = 'E' AND follow_up_status = 6, total, 0)) AS Restarts,
                            SUM(IF(TI = 'TI' AND New = 'E' AND follow_up_status = 5, total, 0))  AS TI,
                            SUM(IF(TI = 'NTI' AND New = 'N' AND follow_up_status = 5, total, 0)) AS New


                     from (select Count(*) as total,
                                  TI,
                                  new,
                                  n.follow_up_status
                           from (select latest.encounter_id                                            as fid,
                                        latest.art_start_date,
                                        case latest.follow_up_status
                                            WHEN 'Transferred out' THEN 0
                                            WHEN 'Stop all' THEN 1
                                            WHEN 'Loss to follow-up (LTFU)' THEN 2
                                            WHEN 'Ran away' THEN 3
                                            WHEN 'Dead' THEN 4
                                            WHEN 'Alive' THEN 5
                                            WHEN 'Restart medication'
                                                THEN 6 END                                             as follow_up_status,
                                        latest.client_id,
                                        latest.art_dose_end,
                                        latest.follow_up_date,
                                        previous.encounter_id,
                                        CASE
                                            WHEN latest.art_start_date <= REPORT_END_DATE AND
                                                 latest.art_start_date > REPORT_START_DATE THEN 'N'
                                            ELSE 'E'
                                            END                                                        AS new,
                                        fn_get_ti_status(latest.client_id, REPORT_END_DATE, REPORT_START_DATE) AS TI,
                                        CASE
                                            WHEN previous.encounter_id IS NULL THEN 'Not counted'
                                            ELSE 'counted'
                                            END                                                        AS expr
                                 from (SELECT d.encounter_id     AS encounter_id,
                                              d.client_id        AS client_id,
                                              d.art_start_date   AS art_start_date,
                                              d.follow_up_status AS follow_up_status,
                                              d.art_dose_end     AS art_dose_end,
                                              d.follow_up_date   AS follow_up_date,
                                              d.next_visit_date  AS next_visit_date
                                       FROM FollowUp AS d
                                                INNER JOIN latest_follow_up ON d.encounter_id = latest_follow_up.encounter_id
                                       WHERE d.follow_up_status IN ('Alive', 'Restart medication')) as latest

                                          LEFT JOIN (SELECT d.encounter_id,
                                                            d.art_dose_end,
                                                            d.client_id,
                                                            d.follow_up_date,
                                                            d.follow_up_status,
                                                            d.art_start_date,
                                                            d.next_visit_date
                                                     FROM FollowUp AS d
                                                              INNER JOIN previous_follow_up ON d.encounter_id = previous_follow_up.encounter_id
                                                     WHERE d.follow_up_status IN ('Alive', 'Restart medication')
                                                       AND d.art_start_date <= REPORT_START_DATE
                                                       AND d.follow_up_date <= REPORT_START_DATE
                                                       AND d.art_dose_end >= REPORT_START_DATE) AS previous
                                                    ON latest.client_id = previous.client_id
                                 WHERE latest.art_start_date <= REPORT_END_DATE
                                   AND latest.follow_up_date <= REPORT_END_DATE
                                   AND latest.art_dose_end >= REPORT_END_DATE
                                   AND previous.encounter_id IS NULL) as n
                           group by TI, new, n.follow_up_status) as tb_a),
-- TO BE ADDED PEDI
     to_be_added_pedi AS (select SUM(IF(TI = 'NTI' AND New = 'E' AND follow_up_status = 5, total, 0)) AS Traced_BackPedi,
                            SUM(IF(TI = 'NTI' AND New = 'E' AND follow_up_status = 6, total, 0)) AS RestartsPedi,
                            SUM(IF(TI = 'TI' AND New = 'E' AND follow_up_status = 5, total, 0))  AS TIPedi,
                            SUM(IF(TI = 'NTI' AND New = 'N' AND follow_up_status = 5, total, 0)) AS NewPedi


                     from (select Count(*) as total,
                                  TI,
                                  new,
                                  n.follow_up_status
                           from (select latest.encounter_id                                            as fid,
                                        latest.art_start_date,
                                        case latest.follow_up_status
                                            WHEN 'Transferred out' THEN 0
                                            WHEN 'Stop all' THEN 1
                                            WHEN 'Loss to follow-up (LTFU)' THEN 2
                                            WHEN 'Ran away' THEN 3
                                            WHEN 'Dead' THEN 4
                                            WHEN 'Alive' THEN 5
                                            WHEN 'Restart medication'
                                                THEN 6 END                                             as follow_up_status,
                                        latest.client_id,
                                        latest.art_dose_end,
                                        latest.follow_up_date,
                                        previous.encounter_id,
                                        CASE
                                            WHEN latest.art_start_date <= REPORT_END_DATE AND
                                                 latest.art_start_date > REPORT_START_DATE THEN 'N'
                                            ELSE 'E'
                                            END                                                        AS new,
                                        fn_get_ti_status(latest.client_id, REPORT_END_DATE, REPORT_START_DATE) AS TI,
                                        CASE
                                            WHEN previous.encounter_id IS NULL THEN 'Not counted'
                                            ELSE 'counted'
                                            END                                                        AS expr
                                 from (SELECT d.encounter_id     AS encounter_id,
                                              d.client_id        AS client_id,
                                              d.art_start_date   AS art_start_date,
                                              d.follow_up_status AS follow_up_status,
                                              d.art_dose_end     AS art_dose_end,
                                              d.follow_up_date   AS follow_up_date,
                                              d.next_visit_date  AS next_visit_date
                                       FROM FollowUp AS d
                                                INNER JOIN latest_follow_up ON d.encounter_id = latest_follow_up.encounter_id
                                       WHERE d.follow_up_status IN ('Alive', 'Restart medication')and age <15) as latest

                                          LEFT JOIN (SELECT d.encounter_id,
                                                            d.art_dose_end,
                                                            d.client_id,
                                                            d.follow_up_date,
                                                            d.follow_up_status,
                                                            d.art_start_date,
                                                            d.next_visit_date
                                                     FROM FollowUp AS d
                                                              INNER JOIN previous_follow_up ON d.encounter_id = previous_follow_up.encounter_id
                                                     WHERE d.follow_up_status IN ('Alive', 'Restart medication')
                                                       AND d.art_start_date <= REPORT_START_DATE
                                                       AND d.follow_up_date <= REPORT_START_DATE
                                                       AND d.art_dose_end >= REPORT_START_DATE) AS previous
                                                    ON latest.client_id = previous.client_id
                                 WHERE latest.art_start_date <= REPORT_END_DATE
                                   AND latest.follow_up_date <= REPORT_END_DATE
                                   AND latest.art_dose_end >= REPORT_END_DATE
                                   AND previous.encounter_id IS NULL) as n
                           group by TI, new, n.follow_up_status) as tb_a),


-- TO BE DEDUCTED

-- Consolidated temp CTE for row number calculation
     temp_latest_d AS (SELECT encounter_id,
                              client_id,
                              follow_up_date                                                                             AS FollowupDate,
                              ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                       FROM FollowUp
                       WHERE follow_up_status IS NOT NULL
                         AND art_start_date IS NOT NULL
                         AND follow_up_date <= REPORT_END_DATE),

-- Select the latest follow-up per client
     latest_follow_up_d AS (SELECT *
                            FROM temp_latest_d
                            WHERE row_num = 1),


-- Consolidated temp CTE for row number calculation
     temp_previous_d AS (SELECT encounter_id,
                                client_id,
                                follow_up_date                                                                             AS FollowupDate,
                                ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                         FROM FollowUp
                         WHERE follow_up_status IS NOT NULL
                           AND art_start_date IS NOT NULL
                           AND follow_up_date <= REPORT_START_DATE),

-- Select the latest follow-up per client
     previous_follow_up_d AS (SELECT *
                              FROM temp_previous_d
                              WHERE row_num = 1),

     to_be_deducted AS (SELECT SUM(IF(follow_up_status = 0, total, 0)) AS TOs,
                               SUM(IF(follow_up_status = 2, total, 0)) AS Losts,
                               SUM(IF(follow_up_status = 3, total, 0)) AS Drops,
                               SUM(IF(follow_up_status = 4, total, 0)) AS Deads,
                               SUM(IF(follow_up_status = 1, total, 0)) AS Stops,
                               SUM(IF(follow_up_status = 5, total, 0)) AS Not_Updated
                        from (SELECT COUNT(*)                                 AS total,
                                     case fb.follow_up_status
                                         WHEN 'Transferred out' THEN 0
                                         WHEN 'Stop all' THEN 1
                                         WHEN 'Loss to follow-up (LTFU)' THEN 2
                                         WHEN 'Ran away' THEN 3
                                         WHEN 'Dead' THEN 4
                                         WHEN 'Alive' THEN 5
                                         WHEN 'Restart medication' THEN 6 END as follow_up_status
                              FROM (SELECT previous.encounter_id  AS fid,
                                           previous.client_id,
                                           previous.art_dose_end,
                                           previous.follow_up_date,
                                           latest.encounter_id,
                                           CASE
                                               WHEN latest.encounter_id IS NULL THEN 'Not counted'
                                               ELSE 'counted' END AS expr
                                    FROM (SELECT c.encounter_id,
                                                 c.client_id,
                                                 c.art_start_date AS ARTstartedDate,
                                                 c.follow_up_status,
                                                 c.art_dose_end,
                                                 c.follow_up_date
                                          FROM FollowUp AS c
                                                   INNER JOIN previous_follow_up_d
                                                              ON c.encounter_id = previous_follow_up_d.encounter_id
                                          WHERE c.follow_up_status IN ('Alive', 'Restart medication')) AS previous
                                             LEFT JOIN (SELECT d.encounter_id,
                                                               d.art_dose_end,
                                                               d.client_id,
                                                               d.follow_up_date,
                                                               d.follow_up_status,
                                                               d.art_start_date,
                                                               d.next_visit_date
                                                        FROM FollowUp AS d
                                                                 INNER JOIN latest_follow_up_d ON d.encounter_id = latest_follow_up_d.encounter_id
                                                        WHERE d.follow_up_status IN ('Alive', 'Restart medication')
                                                          AND d.art_start_date <= REPORT_END_DATE
                                                          AND d.follow_up_date <= REPORT_END_DATE
                                                          AND d.art_dose_end >= REPORT_END_DATE) AS latest
                                                       ON previous.client_id = latest.client_id
                                    WHERE previous.ARTstartedDate <= REPORT_START_DATE
                                      AND previous.follow_up_date <= REPORT_START_DATE
                                      AND previous.art_dose_end >= REPORT_START_DATE
                                      AND latest.encounter_id IS NULL) AS n
                                       INNER JOIN (SELECT d.encounter_id,
                                                          d.art_dose_end,
                                                          d.client_id,
                                                          d.follow_up_date,
                                                          d.follow_up_status
                                                   FROM FollowUp AS d
                                                            INNER JOIN latest_follow_up_d ON d.encounter_id = latest_follow_up_d.encounter_id
                                                   WHERE d.art_start_date <= REPORT_END_DATE
                                                     AND d.follow_up_date <= REPORT_END_DATE) AS fb
                                                  ON fb.client_id = n.client_id
                              GROUP BY fb.follow_up_status) as to_be_deducted),

-- TO BE DEDUCTED PEDI

to_be_deducted_pedi AS (SELECT SUM(IF(follow_up_status = 0, total, 0)) AS TOsPedi,
                               SUM(IF(follow_up_status = 2, total, 0)) AS LostsPedi,
                               SUM(IF(follow_up_status = 3, total, 0)) AS DropsPedi,
                               SUM(IF(follow_up_status = 4, total, 0)) AS DeadsPedi,
                               SUM(IF(follow_up_status = 1, total, 0)) AS StopsPedi,
                               SUM(IF(follow_up_status = 5, total, 0)) AS Not_UpdatedPedi
                        from (SELECT COUNT(*)                                 AS total,
                                     case fb.follow_up_status
                                         WHEN 'Transferred out' THEN 0
                                         WHEN 'Stop all' THEN 1
                                         WHEN 'Loss to follow-up (LTFU)' THEN 2
                                         WHEN 'Ran away' THEN 3
                                         WHEN 'Dead' THEN 4
                                         WHEN 'Alive' THEN 5
                                         WHEN 'Restart medication' THEN 6 END as follow_up_status
                              FROM (SELECT previous.encounter_id  AS fid,
                                           previous.client_id,
                                           previous.art_dose_end,
                                           previous.follow_up_date,
                                           latest.encounter_id,
                                           CASE
                                               WHEN latest.encounter_id IS NULL THEN 'Not counted'
                                               ELSE 'counted' END AS expr
                                    FROM (SELECT c.encounter_id,
                                                 c.client_id,
                                                 c.art_start_date AS ARTstartedDate,
                                                 c.follow_up_status,
                                                 c.art_dose_end,
                                                 c.follow_up_date
                                          FROM FollowUp AS c
                                                   INNER JOIN previous_follow_up_d
                                                              ON c.encounter_id = previous_follow_up_d.encounter_id
                                          WHERE c.follow_up_status IN ('Alive', 'Restart medication')) AS previous
                                             LEFT JOIN (SELECT d.encounter_id,
                                                               d.art_dose_end,
                                                               d.client_id,
                                                               d.follow_up_date,
                                                               d.follow_up_status,
                                                               d.art_start_date,
                                                               d.next_visit_date
                                                        FROM FollowUp AS d
                                                                 INNER JOIN latest_follow_up_d ON d.encounter_id = latest_follow_up_d.encounter_id
                                                        WHERE d.follow_up_status IN ('Alive', 'Restart medication')
                                                          AND d.art_start_date <= REPORT_END_DATE
                                                          AND d.follow_up_date <= REPORT_END_DATE
                                                          AND d.art_dose_end >= REPORT_END_DATE) AS latest
                                                       ON previous.client_id = latest.client_id
                                    WHERE previous.ARTstartedDate <= REPORT_START_DATE
                                      AND previous.follow_up_date <= REPORT_START_DATE
                                      AND previous.art_dose_end >= REPORT_START_DATE
                                      AND latest.encounter_id IS NULL) AS n
                                       INNER JOIN (SELECT d.encounter_id,
                                                          d.art_dose_end,
                                                          d.client_id,
                                                          d.follow_up_date,
                                                          d.follow_up_status
                                                   FROM FollowUp AS d
                                                            INNER JOIN latest_follow_up_d ON d.encounter_id = latest_follow_up_d.encounter_id
                                                   WHERE d.art_start_date <= REPORT_END_DATE
                                                     AND d.follow_up_date <= REPORT_END_DATE) AS fb
                                                  ON fb.client_id = n.client_id
                              GROUP BY fb.follow_up_status) as to_be_deducted)
select *
from to_be_added,to_be_deducted,to_be_added_pedi,to_be_deducted_pedi;
