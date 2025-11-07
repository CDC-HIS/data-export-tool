WITH FollowUp AS (select follow_up.encounter_id,
                         follow_up.client_id,
                         follow_up_date_followup_                          as follow_up_date,
                         follow_up_status,
                         art_antiretroviral_start_date                     as art_start_date,
                         regimen_change                                    as switch,
                         date_of_reported_hiv_viral_load                      viral_load_sent_date,
                         date_viral_load_results_received                     viral_load_performed_date,
                         viral_load_test_status,
                         hiv_viral_load                                    as viral_load_count,
                         viral_load_test_indication,
                         hiv_viral_load_status,
                         COALESCE(
                                 at_3436_weeks_of_gestation,
                                 viral_load_after_eac_confirmatory_viral_load_where_initial_v,
                                 viral_load_after_eac_repeat_viral_load_where_initial_viral_l,
                                 every_six_months_until_mtct_ends,
                                 six_months_after_the_first_viral_load_test_at_postnatal_peri,
                                 three_months_after_delivery,
                                 at_the_first_antenatal_care_visit,
                                 annual_viral_load_test,
                                 second_viral_load_test_at_12_months_post_art,
                                 first_viral_load_test_at_6_months_or_longer_post_art,
                                 first_viral_load_test_at_3_months_or_longer_post_art
                         )                                                 AS routine_viral_load_test_indication,
                         COALESCE(
                                 repeat_or_confirmatory_vl_initial_viral_load_greater_than_10,
                                 suspected_antiretroviral_failure
                         )                                                 AS targeted_viral_load_test_indication,
                         date_third_enhanced_adherence_counseling_provided as eac_3,
                         date_second_enhanced_adherence_counseling_provided   eac_2,
                         date_first_enhanced_adherence_counseling_provided    eac_1,
                         weight_text_                                      as weight,
                         date_of_event                                     as hiv_confirmed_date,
                         pregnancy_status,
                         antiretroviral_art_dispensed_dose_i                  dispensed_dose,
                         regimen,
                         next_visit_date,
                         treatment_end_date                                   art_dose_end_date
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
                                     ON follow_up.encounter_id = follow_up_9.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_10 follow_up_10
                                     ON follow_up.encounter_id = follow_up_10.encounter_id),




     tmp_vl_performed_date_1 AS (SELECT encounter_id,
                                        client_id,
                                        viral_load_performed_date,
                                        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_performed_date DESC , FollowUp.encounter_id DESC ) AS row_num
                                 FROM FollowUp
                                 where follow_up_date <= REPORT_END_DATE),
     tmp_vl_performed_date_1_dedup AS (select * from tmp_vl_performed_date_1 where row_num = 1),

     tmp_vl_sent_date AS (SELECT FollowUp.client_id,
                                 viral_load_sent_date                                                                                                          AS VL_Sent_Date,
                                 ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.viral_load_sent_date DESC , FollowUp.encounter_id DESC ) AS row_num
                          FROM FollowUp
                                   Inner Join tmp_vl_performed_date_1_dedup
                                              ON tmp_vl_performed_date_1_dedup.client_id = FollowUp.client_id
                                                  AND (tmp_vl_performed_date_1_dedup.viral_load_performed_date =
                                                       FollowUp.viral_load_performed_date)
                          WHERE FollowUp.follow_up_date <= REPORT_END_DATE
                            and viral_load_sent_date is not null),
     vl_sent_date AS (select *
                      from tmp_vl_sent_date
                      where row_num = 1),

     vl_performed_date AS (SELECT FollowUp.encounter_id,
                                  FollowUp.client_id,
                                  FollowUp.viral_load_performed_date,
                                  FollowUp.viral_load_test_status,
                                  viral_load_count,
                                  CASE
                                      WHEN vl_sent_date.VL_Sent_Date IS NOT NULL
                                          THEN vl_sent_date.VL_Sent_Date
                                      WHEN FollowUp.viral_load_performed_date IS NOT NULL
                                          THEN FollowUp.viral_load_performed_date
                                      Else NULL END                   AS viral_load_ref_date,
                                  routine_viral_load_test_indication  AS routine_viral_load,
                                  targeted_viral_load_test_indication AS target
                           FROM FollowUp
                                    INNER JOIN tmp_vl_performed_date_1_dedup
                                               ON FollowUp.encounter_id = tmp_vl_performed_date_1_dedup.encounter_id
                                    LEFT JOIN vl_sent_date ON FollowUp.client_id = vl_sent_date.client_id)
        ,
     tmp_vl_performed_date_cf AS (SELECT encounter_id,
                                         client_id,
                                         viral_load_performed_date                                                                                                          AS viral_load_perform_date,
                                         ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.viral_load_performed_date DESC , FollowUp.encounter_id DESC ) AS row_num
                                  FROM FollowUp
                                  where FollowUp.hiv_viral_load_status = 'Completed'
                                        and (
                                          (FollowUp.viral_load_test_indication = 'Routine viral load test indication'
                                              and FollowUp.routine_viral_load_test_indication in
                                                  ('Viral load after EAC: repeat viral load where initial viral load greater than 50 and less than 1000 copies per ml',
                                                   'Viral load after EAC: confirmatory viral load where initial viral load greater than 1000 copies per ml'
                                                      ))
                                              OR
                                          (FollowUp.viral_load_test_indication = 'Targeted viral load test indication'
                                              and FollowUp.targeted_viral_load_test_indication in
                                                  ('Repeat or confirmatory VL: initial viral load greater than 1000')
                                              )
                                          )
                                        and FollowUp.viral_load_performed_date >=
                                            FollowUp.viral_load_performed_date
                                        and FollowUp.viral_load_performed_date <= COALESCE(REPORT_END_DATE, CURDATE())

                                  ),
     tmp_vl_performed_date_cf_2 AS (select * from tmp_vl_performed_date_cf where row_num = 1),
     tmp_switch_sub_date AS (SELECT FollowUp.encounter_id,
                                    FollowUp.client_id,
                                    follow_up_date                                                                            as switch_date,
                                    ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY follow_up_date, FollowUp.encounter_id) AS row_num
                             FROM FollowUp
                             JOIN tmp_vl_performed_date_cf_2 cf on FollowUp.client_id = cf.client_id
                             WHERE FollowUp.follow_up_date BETWEEN cf.viral_load_perform_date AND REPORT_END_DATE
                               and switch is not null),
     switch_sub_date AS (select * from tmp_switch_sub_date where row_num = 1),

     tmp_vl_sent_date_cf AS (SELECT FollowUp.client_id,
                                    viral_load_sent_date                                                                                                          AS VL_Sent_Date,
                                    ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.viral_load_sent_date DESC , FollowUp.encounter_id DESC ) AS row_num
                             FROM FollowUp
                                      Inner Join tmp_vl_performed_date_cf_2
                                                 ON tmp_vl_performed_date_cf_2.client_id = FollowUp.client_id
                                                     AND (tmp_vl_performed_date_cf_2.viral_load_perform_date =
                                                          FollowUp.viral_load_performed_date)
                             WHERE FollowUp.follow_up_date <= REPORT_END_DATE
                               and viral_load_sent_date is not null),
     vl_sent_date_cf AS (select *
                         from tmp_vl_sent_date_cf
                         where row_num = 1),


     tmp_vl_performed_date_cf_3 AS (SELECT FollowUp.encounter_id,
                                           FollowUp.client_id,
                                           FollowUp.viral_load_performed_date,
                                           FollowUp.viral_load_test_status,
                                           FollowUp.viral_load_count           AS viral_load_count,
                                           CASE
                                               WHEN vl_sent_date_cf.VL_Sent_Date IS NOT NULL
                                                   THEN vl_sent_date_cf.VL_Sent_Date
                                               WHEN FollowUp.viral_load_performed_date IS NOT NULL
                                                   THEN FollowUp.viral_load_performed_date
                                               Else NULL END                   AS viral_load_ref_date,
                                           routine_viral_load_test_indication  AS routine_viral_load_cf,

                                           targeted_viral_load_test_indication AS target_cf
                                    FROM FollowUp
                                             INNER JOIN tmp_vl_performed_date_cf_2
                                                        ON FollowUp.encounter_id = tmp_vl_performed_date_cf_2.encounter_id
                                             LEFT JOIN vl_sent_date_cf ON FollowUp.client_id = vl_sent_date_cf.client_id),
     tmp_vl_perf_date_eac_1 AS (SELECT FollowUp.client_id,
                                       eac_1                                                                                                          AS Date_EAC_Provided,
                                       ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.eac_1 DESC , FollowUp.encounter_id DESC ) AS row_num
                                FROM FollowUp
                                         INNER JOIN vl_performed_date ON vl_performed_date.client_id = FollowUp.client_id
                                WHERE FollowUp.eac_1 is not null
                                  AND vl_performed_date.viral_load_performed_date <= FollowUp.follow_up_date
                                  AND eac_1 <= REPORT_END_DATE),
     tmp_vl_perf_date_eac_2 AS (SELECT FollowUp.client_id,
                                       eac_2                                                                                                          AS Date_EAC_Provided,
                                       ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.eac_2 DESC , FollowUp.encounter_id DESC ) AS row_num
                                FROM FollowUp
                                         INNER JOIN vl_performed_date ON vl_performed_date.client_id = FollowUp.client_id
                                WHERE FollowUp.eac_2 is not null
                                  AND vl_performed_date.viral_load_performed_date <= FollowUp.follow_up_date
                                  AND eac_2 <= REPORT_END_DATE),
     tmp_vl_perf_date_eac_3 AS (SELECT FollowUp.client_id,
                                       eac_3                                                                                                          AS Date_EAC_Provided,
                                       ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.eac_3 DESC , FollowUp.encounter_id DESC ) as row_num
                                FROM FollowUp
                                         INNER JOIN vl_performed_date ON vl_performed_date.client_id = FollowUp.client_id
                                WHERE FollowUp.eac_3 is not null
                                  AND vl_performed_date.viral_load_performed_date <= FollowUp.follow_up_date
                                  AND eac_3 <= REPORT_END_DATE),
     vl_perf_date_eac_1 AS (select * from tmp_vl_perf_date_eac_1 where row_num = 1),
     vl_perf_date_eac_2 AS (select * from tmp_vl_perf_date_eac_2 where row_num = 1),
     vl_perf_date_eac_3 AS (select * from tmp_vl_perf_date_eac_3 where row_num = 1),
     tmp_latest_follow_up AS (SELECT client_id,
                                     FollowUp.encounter_id,
                                     ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.follow_up_date DESC , FollowUp.encounter_id DESC ) as row_num
                              FROM FollowUp
                              where follow_up_status Is Not Null
                                AND follow_up_date <= REPORT_END_DATE),
     latest_follow_up as (select * from tmp_latest_follow_up where row_num = 1),
     hvl as (SELECT client.patient_uuid                                        as PatientGUID,
                    TIMESTAMPDIFF(YEAR, client.date_of_birth, REPORT_END_DATE) AS age,
                    CASE Sex
                        WHEN 'FEMALE' THEN 'F'
                        WHEN 'MALE' THEN 'M'
                        end                                                    as Sex,
                    f_case.encounter_id                                        as Id,
                    f_case.client_id                                           as PatientId,
                    f_case.weight,
                    f_case.hiv_confirmed_date                                  as date_hiv_confirmed,
                    f_case.art_start_date,
                    f_case.follow_up_date                                      as FollowUpDate,
                    f_case.pregnancy_status                                    as IsPregnant,
                    f_case.dispensed_dose                                      as ARVDispendsedDose,
                    f_case.regimen                                             as art_dose,
                    f_case.next_visit_date,
                    f_case.follow_up_status,
                    f_case.art_dose_end_date                                   as art_dose_End,
                    vlperfdate.viral_load_performed_date                       as viral_load_perform_date,
                    vlperfdate.viral_load_test_status                          as viral_load_status,
                    vlperfdate.viral_load_count,
                    vlsentdate.VL_Sent_Date,
                    vlperfdate.viral_load_ref_date,
                    sub_switch_date.switch_date                                as SwitchDate,
                    date_eac1.Date_EAC_Provided                                as date_eac_provided_1,
                    date_eac2.Date_EAC_Provided                                as date_eac_provided_2,
                    date_eac3.Date_EAC_Provided                                as date_eac_provided_3,
                    vlsentdate_cf.VL_Sent_Date                                 as viral_load_sent_date_cf,
                    vlperfdate_cf.viral_load_performed_date                    as viral_load_perform_date_cf,
                    vlperfdate_cf.viral_load_test_status                       as viral_load_status_cf,
                    vlperfdate_cf.viral_load_count                             as viral_load_count_cf,
                    vlperfdate.routine_viral_load,
                    vlperfdate.target,
                    vlperfdate_cf.routine_viral_load_cf,
                    vlperfdate_cf.target_cf
             FROM FollowUp AS f_case
                      INNER JOIN latest_follow_up ON f_case.encounter_id = latest_follow_up.encounter_id
                      LEFT JOIN mamba_dim_client client on latest_follow_up.client_id = client.client_id
                      LEFT JOIN vl_performed_date as vlperfdate ON vlperfdate.client_id = f_case.client_id
                      LEFT JOIN tmp_vl_performed_date_cf_3 as vlperfdate_cf
                                ON vlperfdate_cf.client_id = f_case.client_id
                      Left join vl_sent_date as vlsentdate ON vlsentdate.client_id = f_case.client_id
                      Left join vl_sent_date_cf as vlsentdate_cf ON vlsentdate_cf.client_id = f_case.client_id
                      Left join vl_perf_date_eac_1 as date_eac1 ON date_eac1.client_id = f_case.client_id
                      Left join vl_perf_date_eac_2 as date_eac2 ON date_eac2.client_id = f_case.client_id
                      Left join vl_perf_date_eac_3 as date_eac3 ON date_eac3.client_id = f_case.client_id
                      Left join switch_sub_date as sub_switch_date ON sub_switch_date.client_id = f_case.client_id)
select Sex              as Sex,
       Weight           as Weight,
       Age              as Age,
       date_hiv_confirmed,
       art_start_date,
       FollowUpDate,
       IsPregnant,
       ARVDispendsedDose,
       art_dose         as art_dose,
       next_visit_date,
       follow_up_status,
       art_dose_End,
       viral_load_perform_date,
       viral_load_status,
       viral_load_count,
       hvl.VL_Sent_Date as viral_load_sent_date,
       viral_load_ref_date,
       routine_viral_load,
       target,
       hvl.SwitchDate   as date_regimen_change,
       date_eac_provided_1,
       date_eac_provided_2,
       date_eac_provided_3,
       viral_load_sent_date_cf,
       viral_load_perform_date_cf,
       viral_load_status_cf,
       viral_load_count_cf,
       routine_viral_load_cf,
       target_cf,
       PatientGUID
from hvl
where hvl.follow_up_status in ('Alive', 'Restart medication')
  and hvl.art_dose_End >= REPORT_END_DATE
  AND art_start_date <= REPORT_END_DATE
  and TIMESTAMPDIFF(DAY, art_start_date, REPORT_END_DATE) >= 0;