WITH FollowUp AS (SELECT follow_up.encounter_id,
                         follow_up.client_id,
                         follow_up_date_followup_                            AS follow_up_date,
                         follow_up_status,
                         art_antiretroviral_start_date                       AS art_start_date,
                         treatment_end_date                                  AS art_dose_end,
                         next_visit_date,
                         TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) as age
                  FROM mamba_flat_encounter_follow_up follow_up
                           LEFT JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                     ON follow_up.encounter_id = follow_up_1.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                     ON follow_up.encounter_id = follow_up_2.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                     ON follow_up.encounter_id = follow_up_3.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_4 follow_up_4
                                     ON follow_up.encounter_id = follow_up_4.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_5 follow_up_5
                                     ON follow_up.encounter_id = follow_up_5.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_6 follow_up_6
                                     ON follow_up.encounter_id = follow_up_6.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_7 follow_up_7
                                     ON follow_up.encounter_id = follow_up_7.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_8 follow_up_8
                                     ON follow_up.encounter_id = follow_up_8.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_9 follow_up_9
                                     ON follow_up.encounter_id = follow_up_9.encounter_id),

     temp_latest AS (SELECT encounter_id,
                            client_id,
                            follow_up_date                                                                             AS FollowupDate,
                            follow_up_status,
                            art_start_date,
                            art_dose_end,
                            age,
                            ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                     FROM FollowUp
                     WHERE follow_up_status IS NOT NULL
                       AND art_start_date IS NOT NULL
                       AND follow_up_date <= REPORT_END_DATE),

-- Select the latest follow-up per client
     latest_follow_up AS (SELECT *
                          FROM temp_latest
                          WHERE row_num = 1
                            and follow_up_status IN ('Alive', 'Restart medication')
                            and art_start_date <= REPORT_END_DATE
                            AND FollowupDate <= REPORT_END_DATE
                            AND art_dose_end >= REPORT_END_DATE),
     latest_follow_up_pedi AS (SELECT *
                               FROM temp_latest
                               WHERE row_num = 1
                                 and follow_up_status IN ('Alive', 'Restart medication')
                                 and age < 15
                                 and art_start_date <= REPORT_END_DATE
                                 AND FollowupDate <= REPORT_END_DATE
                                 AND art_dose_end >= REPORT_END_DATE),
     latest_follow_up_2 AS (SELECT *
                            FROM temp_latest
                            WHERE row_num = 1
                              and art_start_date <= REPORT_END_DATE),

-- Consolidated temp CTE for row number calculation
     temp_previous AS (SELECT encounter_id,
                              client_id,
                              follow_up_date                                                                             AS FollowupDate,
                              follow_up_status,
                              art_start_date,
                              follow_up_date,
                              art_dose_end,
                              age,
                              ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                       FROM FollowUp
                       WHERE follow_up_status IS NOT NULL
                         AND art_start_date IS NOT NULL
                         AND follow_up_date <= date_add(REPORT_START_DATE, INTERVAL -1 DAY)),

-- Select the latest follow-up per client
     previous_follow_up AS (SELECT *
                            FROM temp_previous
                            WHERE row_num = 1
                              and follow_up_status IN ('Alive', 'Restart medication')
                              AND art_start_date <= date_add(REPORT_START_DATE, INTERVAL -1 DAY)
                              AND follow_up_date <= date_add(REPORT_START_DATE, INTERVAL -1 DAY)
                              AND art_dose_end >= date_add(REPORT_START_DATE, INTERVAL -1 DAY)),
     previous_follow_up_pedi AS (SELECT *
                                 FROM temp_previous
                                 WHERE row_num = 1
                                   and follow_up_status IN ('Alive', 'Restart medication')
                                   and age < 15
                                   AND art_start_date <= date_add(REPORT_START_DATE, INTERVAL -1 DAY)
                                   AND follow_up_date <= date_add(REPORT_START_DATE, INTERVAL -1 DAY)
                                   AND art_dose_end >= date_add(REPORT_START_DATE, INTERVAL -1 DAY)),


     to_be_added AS (select SUM(IF(TI = 'NTI' AND New = 'E' AND follow_up_status = 5, total, 0)) AS Traced_Back,
                            SUM(IF(TI = 'NTI' AND New = 'E' AND follow_up_status = 6, total, 0)) AS Restarts,
                            SUM(IF(TI = 'TI' AND New = 'E' AND follow_up_status = 5, total, 0))  AS TI,
                            SUM(IF(TI = 'NTI' AND New = 'N' AND follow_up_status = 5, total, 0)) AS New


                     from (select Count(*) as total,
                                  TI,
                                  new,
                                  n.follow_up_status
                           from (select latest.encounter_id               as fid,
                                        latest.art_start_date,
                                        case latest.follow_up_status
                                            WHEN 'Transferred out' THEN 0
                                            WHEN 'Stop all' THEN 1
                                            WHEN 'Loss to follow-up (LTFU)' THEN 2
                                            WHEN 'Ran away' THEN 3
                                            WHEN 'Dead' THEN 4
                                            WHEN 'Alive' THEN 5
                                            WHEN 'Restart medication'
                                                THEN 6 END                as follow_up_status,
                                        latest.client_id,
                                        latest.art_dose_end,
                                        latest.FollowupDate,
                                        previous.encounter_id,
                                        CASE
                                            WHEN
                                                latest.art_start_date > date_add(REPORT_START_DATE, INTERVAL -1 DAY) AND
                                                latest.art_start_date <= REPORT_END_DATE
                                                THEN 'N'
                                            ELSE 'E'
                                            END                           AS new,
                                        fn_get_ti_status(latest.client_id, date_add(REPORT_START_DATE, INTERVAL -1 DAY),
                                                         REPORT_END_DATE) AS TI,
                                        CASE
                                            WHEN previous.encounter_id IS NULL THEN 'Not counted'
                                            ELSE 'counted'
                                            END                           AS expr
                                 from FollowUp AS d
                                          INNER JOIN latest_follow_up latest ON d.encounter_id = latest.encounter_id
                                          LEFT JOIN previous_follow_up AS previous ON latest.client_id = previous.client_id
                                 WHERE previous.encounter_id IS NULL) as n
                           group by TI, new, n.follow_up_status) as to_be_added),
     to_be_added_pedi
         as (select SUM(IF(TI = 'NTI' AND New = 'E' AND follow_up_status = 5, total, 0)) AS Traced_BackPedi,
                    SUM(IF(TI = 'NTI' AND New = 'E' AND follow_up_status = 6, total, 0)) AS RestartsPedi,
                    SUM(IF(TI = 'TI' AND New = 'E' AND follow_up_status = 5, total, 0))  AS TIPedi,
                    SUM(IF(TI = 'NTI' AND New = 'N' AND follow_up_status = 5, total, 0)) AS NewPedi


             from (select Count(*) as total,
                          TI,
                          new,
                          n.follow_up_status
                   from (select latest.encounter_id               as fid,
                                latest.art_start_date,
                                case latest.follow_up_status
                                    WHEN 'Transferred out' THEN 0
                                    WHEN 'Stop all' THEN 1
                                    WHEN 'Loss to follow-up (LTFU)' THEN 2
                                    WHEN 'Ran away' THEN 3
                                    WHEN 'Dead' THEN 4
                                    WHEN 'Alive' THEN 5
                                    WHEN 'Restart medication'
                                        THEN 6 END                as follow_up_status,
                                latest.client_id,
                                latest.art_dose_end,
                                latest.FollowupDate,
                                previous.encounter_id,
                                CASE
                                    WHEN latest.art_start_date > date_add(REPORT_START_DATE, INTERVAL -1 DAY) AND
                                         latest.art_start_date <= REPORT_END_DATE
                                        THEN 'N'
                                    ELSE 'E'
                                    END                           AS new,
                                fn_get_ti_status(latest.client_id, date_add(REPORT_START_DATE, INTERVAL -1 DAY),
                                                 REPORT_END_DATE) AS TI,
                                CASE
                                    WHEN previous.encounter_id IS NULL THEN 'Not counted'
                                    ELSE 'counted'
                                    END                           AS expr
                         FROM FollowUp AS d
                                  INNER JOIN latest_follow_up_pedi latest ON d.encounter_id = latest.encounter_id
                                  LEFT JOIN previous_follow_up previous
                                            ON latest.client_id = previous.client_id
                         WHERE previous.encounter_id IS NULL) as n
                   group by TI, new, n.follow_up_status) as to_be_added),
     to_be_deducted as (SELECT SUM(IF(follow_up_status = 0, total, 0)) AS TOs,
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
                                           previous.FollowupDate,
                                           latest.encounter_id,
                                           latest.follow_up_status,
                                           CASE
                                               WHEN latest.encounter_id IS NULL THEN 'Not counted'
                                               ELSE 'counted' END AS expr
                                    FROM FollowUp c
                                             INNER JOIN previous_follow_up previous on c.encounter_id = previous.encounter_id
                                             LEFT JOIN latest_follow_up latest ON previous.client_id = latest.client_id
                                    WHERE latest.encounter_id IS NULL) AS n
                                       INNER JOIN latest_follow_up_2 AS fb
                                                  ON fb.client_id = n.client_id
                              GROUP BY fb.follow_up_status) as to_be_deducted),
     to_be_deducted_pedi as (SELECT SUM(IF(follow_up_status = 0, total, 0)) AS TOsPedi,
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
                                                previous.FollowupDate,
                                                latest.encounter_id,
                                                CASE
                                                    WHEN latest.encounter_id IS NULL THEN 'Not counted'
                                                    ELSE 'counted' END AS expr
                                         FROM FollowUp AS c
                                                  INNER JOIN previous_follow_up_pedi previous
                                                             ON c.encounter_id = previous.encounter_id

                                                  LEFT JOIN latest_follow_up latest ON previous.client_id = latest.client_id
                                         WHERE latest.encounter_id IS NULL) AS n
                                            INNER JOIN latest_follow_up_2 fb ON fb.client_id = n.client_id
                                   GROUP BY fb.follow_up_status) as to_be_deducted)

select *
from to_be_added,
     to_be_deducted,
     to_be_added_pedi,
     to_be_deducted_pedi;
