WITH FollowUp AS (SELECT follow_up.client_id,
                         follow_up.encounter_id,
                         date_viral_load_results_received AS viral_load_perform_date,
                         viral_load_received_,
                         follow_up_status,
                         follow_up_date_followup_         AS follow_up_date,
                         art_antiretroviral_start_date       art_start_date,
                         viral_load_test_status,
                         hiv_viral_load                   AS viral_load_count,
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
                         )                                AS routine_viral_load_test_indication,
                         COALESCE(repeat_or_confirmatory_vl_initial_viral_load_greater_than_10,
                                  suspected_antiretroviral_failure
                         )                                AS targeted_viral_load_test_indication,
                         viral_load_test_indication,
                         pregnancy_status,
                         currently_breastfeeding_child    AS breastfeeding_status,
                         antiretroviral_art_dispensed_dose_i arv_dispensed_dose,
                         regimen,
                         next_visit_date,
                         treatment_end_date,
                         date_of_event                       date_hiv_confirmed,
                         weight_text_                     as weight,
                         date_of_reported_hiv_viral_load  as viral_load_sent_date,
                         regimen_change
                  FROM mamba_flat_encounter_follow_up follow_up
                           JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                ON follow_up.encounter_id = follow_up_1.encounter_id
                           JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                ON follow_up.encounter_id = follow_up_2.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                ON follow_up.encounter_id = follow_up_3.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_4 follow_up_4
                                ON follow_up.encounter_id = follow_up_4.encounter_id),


     tmp_all_art_follow_ups as (SELECT encounter_id,
                                       client_id,
                                       follow_up_status,
                                       follow_up_date                                                                             AS FollowUpDate,
                                       ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                FROM FollowUp
                                WHERE follow_up_date <= REPORT_END_DATE
                                  and follow_up_date >= REPORT_START_DATE),

     all_art_follow_ups as (select * from tmp_all_art_follow_ups where row_num = 1),

     tmp_vl_sent_date as (SELECT encounter_id,
                                 client_id,
                                 viral_load_sent_date                                                                             AS VL_Sent_Date,
                                 ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_sent_date DESC, encounter_id DESC) AS row_num
                          FROM FollowUp
                          WHERE viral_load_sent_date is not null
                            and viral_load_sent_date <= REPORT_END_DATE
                            and viral_load_sent_date >= REPORT_START_DATE),
     vl_sent_date as (select * from tmp_vl_sent_date where row_num = 1),

     tmp_switch_sub_date as (SELECT encounter_id,
                                    client_id,
                                    follow_up_date                                                                             AS FollowUpDate,
                                    ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                             FROM FollowUp
                             WHERE follow_up_date <= REPORT_END_DATE
                               and follow_up_date >= REPORT_START_DATE
                               and regimen_change is not null),
     switch_sub_date as (select * from tmp_switch_sub_date where row_num = 1),

     tmp_vl_performed_date_1 as (SELECT encounter_id,
                                        client_id,
                                        viral_load_perform_date                                                                             AS viral_load_perform_date,
                                        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_perform_date DESC, encounter_id DESC) AS row_num
                                 FROM FollowUp
                                 WHERE art_start_date IS NOT NULL
                                   AND (
                                     (viral_load_perform_date IS NOT NULL AND
                                      viral_load_perform_date <= REPORT_END_DATE
                                         AND viral_load_perform_date
                                          >= REPORT_START_DATE)
                                         OR
                                     viral_load_perform_date IS NULL
                                     )),
     tmp_vl_performed_date_2 as (select * from tmp_vl_performed_date_1 where row_num = 1),

     tmp_vl_performed_date_3 as (SELECT FollowUp.encounter_id,
                                        FollowUp.client_id,
                                        case
                                            when FollowUp.viral_load_perform_date < vl_sent_date.VL_Sent_Date then null
                                            else FollowUp.viral_load_perform_date end as viral_load_perform_date,
                                        case
                                            when FollowUp.viral_load_perform_date < vl_sent_date.VL_Sent_Date then null
                                            else FollowUp.viral_load_test_status end  as viral_load_status,
                                        CASE
                                            WHEN FollowUp.viral_load_count > 0 AND
                                                 FollowUp.viral_load_perform_date >= vl_sent_date.VL_Sent_Date
                                                THEN CAST(FollowUp.viral_load_count AS DECIMAL(12, 2))
                                            ELSE NULL END                             AS viral_load_count,
                                        CASE
                                            WHEN
                                                viral_load_test_status IS NULL AND
                                                FollowUp.viral_load_perform_date >= vl_sent_date.VL_Sent_Date
                                                THEN
                                                NULL
                                            WHEN FollowUp.viral_load_perform_date >= vl_sent_date.VL_Sent_Date AND
                                                 (viral_load_test_status LIKE 'Det%'
                                                     OR viral_load_test_status LIKE 'Uns%'
                                                     OR viral_load_test_status LIKE 'High VL%'
                                                     OR viral_load_test_status LIKE 'Low Level Viremia%')
                                                THEN
                                                'U'
                                            WHEN FollowUp.viral_load_perform_date >= vl_sent_date.VL_Sent_Date AND
                                                 (viral_load_test_status LIKE 'Su%'
                                                     OR viral_load_test_status LIKE 'Undet%')
                                                THEN
                                                'S'
                                            WHEN
                                                FollowUp.viral_load_perform_date >= vl_sent_date.VL_Sent_Date AND
                                                (ISNULL(viral_load_count) > CAST(50 AS float)
                                                    )
                                                THEN
                                                'U'
                                            WHEN
                                                FollowUp.viral_load_perform_date >= vl_sent_date.VL_Sent_Date AND
                                                (ISNULL(viral_load_count) <= CAST(50 AS float)
                                                    )
                                                THEN
                                                'S'
                                            ELSE
                                                NULL
                                            END                                       AS viral_load_status_inferred,
                                        CASE
                                            WHEN vl_sent_date.VL_Sent_Date IS NOT NULL
                                                THEN vl_sent_date.VL_Sent_Date
                                            WHEN FollowUp.viral_load_perform_date IS NOT NULL
                                                THEN FollowUp.viral_load_perform_date
                                            ELSE NULL END                             AS viral_load_ref_date,
                                        FollowUp.routine_viral_load_test_indication
                                 FROM FollowUp
                                          INNER JOIN tmp_vl_performed_date_2
                                                     ON FollowUp.encounter_id = tmp_vl_performed_date_2.encounter_id
                                          LEFT JOIN vl_sent_date
                                                    ON FollowUp.client_id = vl_sent_date.client_id),
     tmp_latest_alive_restart as (SELECT encounter_id,
                                         client_id,
                                         follow_up_date                                                                             AS FollowupDate,
                                         ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  WHERE follow_up_status in ('Alive', 'Restart medication')
                                    AND follow_up_date <= REPORT_END_DATE
                                    AND follow_up_date >= REPORT_START_DATE),
     latest_alive_restart as (select * from tmp_latest_alive_restart where row_num = 1),
     vl_eligibility as (SELECT f_case.art_start_date                         as art_start_date,
                               f_case.breastfeeding_status                   as BreastFeeding,
                               f_case.date_hiv_confirmed                     as date_hiv_confirmed,
                               sub_switch_date.FollowupDate                  as date_regimen_change,
                               all_art_follow_ups.follow_up_status           as follow_up_status,
                               f_case.follow_up_date                         as FollowUpDate,
                               patient_name                                  as FullName,
                               f_case.pregnancy_status                       as IsPregnant,
                               mobile_no                                     as MobilePhoneNumber,
                               mrn                                           as MRN,
                               patient_uuid                                  as PatientGUID,
                               f_case.client_id                              as PatientId,
                               CASE Sex
                                   WHEN 'FEMALE' THEN 'F'
                                   WHEN 'MALE' THEN 'M'
                                   end                                       as Sex,
                               vlperfdate.viral_load_count                   as viral_load_count,
                               vlperfdate.viral_load_perform_date            as viral_load_perform_date,
                               vlsentdate.VL_Sent_Date                       as viral_load_sent_date,
                               vlperfdate.viral_load_status                  as viral_load_status,
                               current_age,
                               f_case.weight,
                               arv_dispensed_dose,
                               f_case.regimen,
                               f_case.next_visit_date,
                               f_case.treatment_end_date,
                               CASE
                                   WHEN vlsentdate.VL_Sent_Date IS NOT NULL
                                       THEN vlsentdate.VL_Sent_Date
                                   WHEN vlperfdate.viral_load_perform_date IS NOT NULL
                                       THEN vlperfdate.viral_load_perform_date
                                   ELSE NULL END                             AS viral_load_ref_date,
                               sub_switch_date.FollowupDate                  as switchDate,
                               vlperfdate.viral_load_status_inferred,

                               vlperfdate.routine_viral_load_test_indication as viral_load_indication,

                               CASE

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NULL
                                           AND f_case.follow_up_status = 'Restart medication')
                                       THEN DATE_ADD(f_case.follow_up_date, INTERVAL 91 DAY)

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NULL
                                           AND sub_switch_date.FollowupDate IS NOt NULL
                                           )
                                       THEN DATE_ADD(sub_switch_date.FollowupDate, INTERVAL 181 DAY)

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NULL
                                           AND f_case.pregnancy_status = 'Yes'
                                           AND TIMESTAMPDIFF(DAY, f_case.art_start_date, REPORT_END_DATE) > 90)
                                       THEN DATE_ADD(f_case.art_start_date, INTERVAL 91 DAY)

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NULL
                                           AND TIMESTAMPDIFF(DAY, f_case.art_start_date, REPORT_END_DATE) <= 180)
                                       THEN NULL


                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NULL
                                           AND TIMESTAMPDIFF(DAY, f_case.art_start_date, REPORT_END_DATE) > 180)
                                       THEN DATE_ADD(f_case.art_start_date, INTERVAL 181 DAY)

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NOT NULL
                                           AND vlperfdate.viral_load_ref_date < f_case.follow_up_date)
                                           AND (f_case.follow_up_status = 'Restart medication')
                                       THEN DATE_ADD(f_case.follow_up_date, INTERVAL 91 DAY)

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NOT NULL
                                           AND vlperfdate.viral_load_ref_date < sub_switch_date.FollowupDate
                                           AND sub_switch_date.FollowupDate IS NOT NULL
                                           )
                                       THEN DATE_ADD(sub_switch_date.FollowupDate, INTERVAL 181 DAY)

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NOT NULL
                                           AND vlperfdate.viral_load_status_inferred = 'U')
                                       THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 91 DAY)

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NOT NULL
                                           AND (f_case.pregnancy_status = 'Yes' OR f_case.breastfeeding_status = 'Yes')
                                           AND vlperfdate.routine_viral_load_test_indication in
                                               ('First viral load test at 6 months or longer post ART',
                                                'Viral load after EAC: repeat viral load where initial viral load greater than 50 and less than 1000 copies per ml',
                                                'Viral load after EAC: confirmatory viral load where initial viral load greater than 1000 copies per ml'))
                                       THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 91 DAY)

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NOT NULL
                                           AND (f_case.pregnancy_status = 'Yes' OR f_case.breastfeeding_status = 'Yes')
                                           AND vlperfdate.routine_viral_load_test_indication IS NOT NULL
                                           AND vlperfdate.routine_viral_load_test_indication not in
                                               ('First viral load test at 6 months or longer post ART',
                                                'Viral load after EAC: repeat viral load where initial viral load greater than 50 and less than 1000 copies per ml',
                                                'Viral load after EAC: confirmatory viral load where initial viral load greater than 1000 copies per ml'))
                                       THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 181 DAY)


                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NOT NULL)
                                       THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 365 DAY)

                                   ELSE '12-31-9999' End                     AS eligiblityDate,

                               CASE

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NULL
                                           AND f_case.follow_up_status = 'Restart medication')
                                       THEN 'client restarted ART'

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NULL
                                           AND sub_switch_date.FollowupDate IS NOt NULL
                                           )
                                       THEN 'Regimen Change'


                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NULL
                                           AND f_case.pregnancy_status = 'Yes'
                                           AND TIMESTAMPDIFF(DAY, f_case.art_start_date, REPORT_END_DATE) > 90)
                                       THEN 'First VL for Pregnant'

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NULL
                                           AND TIMESTAMPDIFF(DAY, f_case.art_start_date, REPORT_END_DATE) <= 180)
                                       THEN 'N/A'

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NULL
                                           AND TIMESTAMPDIFF(DAY, f_case.art_start_date, REPORT_END_DATE) > 180)
                                       THEN 'First VL'


                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NOT NULL
                                           AND vlperfdate.viral_load_ref_date < f_case.follow_up_date)
                                           AND (f_case.follow_up_status = 'Restart medication')
                                       THEN 'client restarted ART'

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NOT NULL
                                           AND vlperfdate.viral_load_ref_date < sub_switch_date.FollowupDate
                                           AND sub_switch_date.FollowupDate IS NOT NULL
                                           )
                                       THEN 'Regimen Change'

                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NOT NULL
                                           AND vlperfdate.viral_load_status_inferred = 'U')
                                       THEN 'Repeat/Confirmatory Viral Load test'

                                   WHEN
                                       (vlperfdate.viral_load_status_inferred IS NOT NULL
                                           AND (f_case.pregnancy_status = 'Yes' OR f_case.breastfeeding_status = 'Yes'))
                                       THEN 'Pregnant/Breastfeeding and needs retesting'


                                   WHEN
                                       (vlperfdate.viral_load_ref_date IS NOT NULL)
                                       THEN 'Annual Viral Load Test'

                                   ELSE 'Unassigned' End                     AS vl_status_final

                        FROM FollowUp AS f_case
                                 INNER JOIN latest_alive_restart
                                            ON f_case.encounter_id = latest_alive_restart.encounter_id
                                 LEFT JOIN mamba_dim_client client on latest_alive_restart.client_id = client.client_id
                                 LEFT JOIN tmp_vl_performed_date_3 as vlperfdate
                                           ON vlperfdate.client_id = f_case.client_id

                                 Left join vl_sent_date as vlsentdate
                                           ON vlsentdate.client_id = f_case.client_id

                                 Left join switch_sub_date as sub_switch_date
                                           ON sub_switch_date.client_id = f_case.client_id


                                 Left join all_art_follow_ups on f_case.client_id = all_art_follow_ups.client_id

                        where all_art_follow_ups.follow_up_status in ('Alive', 'Restart Medication') and   TIMESTAMPDIFF(DAY, f_case.art_start_date, REPORT_END_DATE) >=0)
select t.Sex,
       Weight,
       TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) as Age,
       date_hiv_confirmed,
       art_start_date,
       FollowUpDate,
       IsPregnant,
       regimen              as ARVDispendsedDose,
       t.arv_dispensed_dose as ARTDoseDays,
       next_visit_date,
       follow_up_status,
       treatment_end_date   as art_dose_End,
       viral_load_perform_date,
       viral_load_status,
       viral_load_count,
       viral_load_sent_date,
       viral_load_ref_date,
       date_regimen_change,
       eligiblityDate,
       PatientGUID,
       t.BreastFeeding      as IsBreastfeeding,
       vl_status_final,
       CASE
           WHEN IsPregnant = 'Yes' THEN 'Yes'
           WHEN BreastFeeding = 'Yes' THEN 'Yes'
           ELSE 'No' END    AS PMTCT_ART
from vl_eligibility t
    left join mamba_dim_client client on t.PatientId = client.client_id
;