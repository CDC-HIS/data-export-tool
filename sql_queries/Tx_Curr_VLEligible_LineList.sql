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

     tmp_vl_sent_date as (SELECT client_id,
                                 encounter_id,
                                 viral_load_sent_date                                                                             as VL_Sent_Date,
                                 ROW_NUMBER() over (PARTITION BY client_id ORDER BY viral_load_sent_date DESC, encounter_id DESC) AS row_num
                          FROM FollowUp
                          WHERE follow_up_date <= REPORT_END_DATE),
     vl_sent_date as (select * from tmp_vl_sent_date where row_num = 1),
     tmp_switch_sub_date as (SELECT client_id,
                                    encounter_id,
                                    follow_up_date,
                                    ROW_NUMBER() over (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                             FROM FollowUp
                             WHERE follow_up_date <= REPORT_END_DATE
                               AND regimen_change is not null),
     switch_sub_date as (select * from tmp_switch_sub_date where row_num = 1),
     tmp_vl_performed_date as (SELECT client_id,
                                      encounter_id,
                                      viral_load_perform_date,
                                      ROW_NUMBER() over (PARTITION BY client_id ORDER BY viral_load_perform_date DESC, encounter_id DESC) AS row_num
                               FROM FollowUp
                               WHERE follow_up_date <= REPORT_END_DATE),
     vl_performed_date as (select *
                           from tmp_vl_performed_date
                           where row_num = 1),
     vl_performed_date_2 as (SELECT ds.encounter_id,
                                    ds.client_id,
                                    ds.viral_load_perform_date,
                                    ds.viral_load_test_status,
                                    ds.viral_load_count AS viral_load_count,
                                    CASE
                                        WHEN vl_sent_date.VL_Sent_Date IS NOT NULL AND
                                             vl_sent_date.VL_Sent_Date is not null
                                            THEN vl_sent_date.VL_Sent_Date
                                        WHEN ds.viral_load_perform_date IS NOT NULL AND
                                             ds.viral_load_perform_date is not null
                                            THEN ds.viral_load_perform_date
                                        Else NULL END   AS viral_load_ref_date
                             FROM FollowUp AS ds
                                      INNER JOIN vl_performed_date ON ds.encounter_id = vl_performed_date.encounter_id
                                      LEFT JOIN vl_sent_date ON ds.client_id = vl_sent_date.client_id),
     tmp_latest_follow_up as (SELECT client_id,
                                     encounter_id,
                                     follow_up_date,
                                     ROW_NUMBER() over (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                              FROM FollowUp
                              WHERE follow_up_date <= REPORT_END_DATE),
     latest_follow_up as (select * from tmp_latest_follow_up where row_num = 1),

     vl_eligible as (SELECT client.patient_uuid                              as PatientGUID,
                            TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) AS age,
                            client.sex                                       as Sex,
                            f_case.encounter_id,
                            f_case.client_id                                 as PatientId,
                            f_case.weight                                    as Weight,
                            f_case.date_hiv_confirmed,
                            f_case.art_start_date,
                            f_case.follow_up_date                            as FollowUpDate,
                            f_case.pregnancy_status                          as IsPregnant,
                            left(f_case.regimen,2)                                   as ARVDispendsedDose,
                            f_case.arv_dispensed_dose                        as art_dose,
                            f_case.next_visit_date,
                            f_case.follow_up_status,
                            f_case.treatment_end_date                        as art_dose_End,
                            vlperfdate.viral_load_perform_date,
                            vlperfdate.viral_load_test_status                as viral_load_status,
                            vlperfdate.viral_load_count,
                            vlsentdate.VL_Sent_Date,
                            vlperfdate.viral_load_ref_date,
                            sub_switch_date.follow_up_date                   as SwitchFollowupDate,
                            CASE
                                WHEN ((TIMESTAMPDIFF(DAY, f_case.art_start_date, REPORT_END_DATE) <= 12) AND
                                      (f_case.pregnancy_status is null OR f_case.pregnancy_status = 'No') AND
                                      sub_switch_date.follow_up_date IS NULL AND
                                      vlperfdate.viral_load_ref_date IS NULL AND
                                      vlperfdate.viral_load_count IS NULL)
                                    THEN DATE_ADD(f_case.art_start_date, INTERVAL 181 DAY)
                                WHEN ((TIMESTAMPDIFF(MONTH, f_case.art_start_date, REPORT_END_DATE) <= 12) AND
                                      (f_case.pregnancy_status is null OR f_case.pregnancy_status = 'No') AND
                                      sub_switch_date.follow_up_date IS NULL AND
                                      vlperfdate.viral_load_ref_date IS NOT NULL AND
                                      (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 1000))
                                    THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 181 DAY)
                                WHEN ((TIMESTAMPDIFF(MONTH, f_case.art_start_date, REPORT_END_DATE) > 12) AND
                                      (f_case.pregnancy_status is null OR f_case.pregnancy_status = 'No') AND
                                      (sub_switch_date.follow_up_date IS NULL) AND
                                      (vlperfdate.viral_load_ref_date IS NOT NULL) AND
                                      (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 1000))
                                    THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 365 DAY)
                                WHEN ((TIMESTAMPDIFF(MONTH, f_case.art_start_date, REPORT_END_DATE) > 12) AND
                                      (f_case.pregnancy_status is null OR f_case.pregnancy_status = 'No') AND
                                      (sub_switch_date.follow_up_date IS NULL) AND
                                      (vlperfdate.viral_load_ref_date is null) AND
                                      (vlperfdate.viral_load_count IS NULL))
                                    THEN DATE_ADD(f_case.art_start_date, INTERVAL
                                                  365 * TIMESTAMPDIFF(YEAR, f_case.art_start_date, REPORT_END_DATE) DAY)
                                WHEN ((TIMESTAMPDIFF(MONTH, f_case.art_start_date, REPORT_END_DATE) <= 12) AND
                                      (f_case.pregnancy_status = 'Yes') AND
                                      sub_switch_date.follow_up_date IS NULL AND
                                      vlperfdate.viral_load_ref_date IS NOT NULL AND
                                      (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 1000))
                                    THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 91 DAY)
                                WHEN (TIMESTAMPDIFF(MONTH, f_case.art_start_date, REPORT_END_DATE) <= 12) AND
                                     (f_case.pregnancy_status = 'Yes' AND
                                      sub_switch_date.follow_up_date IS NULL AND
                                      vlperfdate.viral_load_ref_date IS NULL AND
                                      (vlperfdate.viral_load_count IS NULL))
                                    THEN DATE_ADD(f_case.art_start_date, INTERVAL 91 DAY)
                                WHEN ((TIMESTAMPDIFF(MONTH, f_case.art_start_date, REPORT_END_DATE) > 12) AND
                                      (f_case.pregnancy_status = 'Yes') AND
                                      sub_switch_date.follow_up_date IS NULL AND
                                      vlperfdate.viral_load_ref_date IS NULL AND
                                      (vlperfdate.viral_load_count IS NULL)) THEN REPORT_END_DATE
                                WHEN ((TIMESTAMPDIFF(MONTH, f_case.art_start_date, REPORT_END_DATE) > 12) AND
                                      (f_case.pregnancy_status = 'Yes') AND
                                      sub_switch_date.follow_up_date IS NULL AND
                                      (vlperfdate.viral_load_ref_date IS NOT NULL AND
                                       vlperfdate.viral_load_ref_date != '1900-01-01 00:00:00.000') AND
                                      (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 1000))
                                    THEN REPORT_END_DATE
                                WHEN ((TIMESTAMPDIFF(MONTH, f_case.art_start_date, REPORT_END_DATE) > 12) AND
                                      (f_case.pregnancy_status is null OR f_case.pregnancy_status = 'No') AND
                                      sub_switch_date.follow_up_date IS NULL AND
                                      vlperfdate.viral_load_ref_date IS NOT NULL AND
                                      (vlperfdate.viral_load_count >= 1000))
                                    THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 121 DAY)
                                WHEN ((TIMESTAMPDIFF(MONTH, f_case.art_start_date, REPORT_END_DATE) <= 12) AND
                                      (f_case.pregnancy_status is null OR f_case.pregnancy_status = 'No') AND
                                      sub_switch_date.follow_up_date IS NULL AND
                                      vlperfdate.viral_load_ref_date IS NOT NULL AND
                                      vlperfdate.viral_load_count >= 1000)
                                    THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 121 DAY)
                                WHEN ((TIMESTAMPDIFF(MONTH, f_case.art_start_date, REPORT_END_DATE) > 12) AND
                                      sub_switch_date.follow_up_date IS NOT NULL AND
                                      vlperfdate.viral_load_ref_date IS NOT NULL AND
                                      (vlperfdate.viral_load_ref_date >= sub_switch_date.follow_up_date) AND
                                      (vlperfdate.viral_load_count < 1000 OR vlperfdate.viral_load_count is null))
                                    THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 365 DAY)
                                WHEN ((TIMESTAMPDIFF(MONTH, f_case.art_start_date, REPORT_END_DATE) > 12) AND
                                      sub_switch_date.follow_up_date IS NOT NULL AND
                                      vlperfdate.viral_load_ref_date IS NOT NULL AND
                                      (vlperfdate.viral_load_ref_date >= sub_switch_date.follow_up_date) AND
                                      (vlperfdate.viral_load_count >= 1000))
                                    THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 121 DAY)
                                WHEN ((TIMESTAMPDIFF(MONTH, f_case.art_start_date, REPORT_END_DATE) > 12) AND
                                      sub_switch_date.follow_up_date IS NOT NULL AND
                                      vlperfdate.viral_load_ref_date IS NOT NULL AND
                                      (vlperfdate.viral_load_ref_date < sub_switch_date.follow_up_date))
                                    THEN DATE_ADD(sub_switch_date.follow_up_date, INTERVAL 181 DAY)
                                WHEN ((TIMESTAMPDIFF(MONTH, f_case.art_start_date, REPORT_END_DATE) > 12) AND
                                      sub_switch_date.follow_up_date IS NOT NULL AND
                                      vlperfdate.viral_load_ref_date IS NULL)
                                    THEN DATE_ADD(sub_switch_date.follow_up_date, INTERVAL 181 DAY)
                                WHEN ((TIMESTAMPDIFF(MONTH, f_case.art_start_date, REPORT_END_DATE) < 12) AND
                                      sub_switch_date.follow_up_date IS NOT NULL AND
                                      vlperfdate.viral_load_ref_date IS NOT NULL AND
                                      (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 1000) AND
                                      (vlperfdate.viral_load_ref_date >= sub_switch_date.follow_up_date) AND
                                      (vlperfdate.viral_load_count < 1000 OR vlperfdate.viral_load_count is null))
                                    THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 365 DAY)
                                WHEN ((TIMESTAMPDIFF(MONTH, f_case.art_start_date, REPORT_END_DATE) < 12) AND
                                      sub_switch_date.follow_up_date IS NOT NULL AND
                                      vlperfdate.viral_load_ref_date IS NOT NULL AND
                                      (vlperfdate.viral_load_ref_date >= sub_switch_date.follow_up_date) AND
                                      (vlperfdate.viral_load_count >= 1000))
                                    THEN DATE_ADD(vlperfdate.viral_load_ref_date, INTERVAL 91 DAY)
                                WHEN ((TIMESTAMPDIFF(MONTH, f_case.art_start_date, REPORT_END_DATE) < 12) AND
                                      sub_switch_date.follow_up_date IS NOT NULL AND
                                      vlperfdate.viral_load_ref_date IS NOT NULL AND
                                      (vlperfdate.viral_load_count IS NULL OR vlperfdate.viral_load_count < 1000) and
                                      (vlperfdate.viral_load_ref_date < sub_switch_date.follow_up_date))
                                    THEN DATE_ADD(sub_switch_date.follow_up_date, INTERVAL 181 DAY)
                                WHEN ((TIMESTAMPDIFF(MONTH, f_case.art_start_date, REPORT_END_DATE) < 12) AND
                                      sub_switch_date.follow_up_date IS NOT NULL AND
                                      vlperfdate.viral_load_ref_date IS NULL)
                                    THEN DATE_ADD(sub_switch_date.follow_up_date, INTERVAL 181 DAY)
                                Else '' End                                  AS eligiblityDate
                     FROM FollowUp AS f_case
                              INNER JOIN latest_follow_up ON f_case.encounter_id = latest_follow_up.encounter_id
                              LEFT JOIN vl_performed_date_2 as vlperfdate ON vlperfdate.client_id = f_case.client_id
                              Left join vl_sent_date as vlsentdate ON vlsentdate.client_id = f_case.client_id
                              Left join switch_sub_date as sub_switch_date
                                        ON sub_switch_date.client_id = f_case.client_id
                              inner join mamba_dim_client as client
                                         ON client.client_id = f_case.client_id)

select Sex as Sex,
       Weight as Weight,
       age as age,
       date_hiv_confirmed as date_hiv_confirmed,
       art_start_date,
       FollowUpDate,
       IsPregnant,
       ARVDispendsedDose,
       art_dose,
       next_visit_date,
       follow_up_status,
       art_dose_End,
       viral_load_perform_date,
       viral_load_status,
       viral_load_count,
       VL_Sent_Date as viral_load_sent_date,
       viral_load_ref_date,
       SwitchFollowupDate as date_regimen_change,
       eligiblityDate,
       PatientGUID as PatientGUID
from vl_eligible
where vl_eligible.follow_up_status in ('Alive', 'Restart medication')
  and vl_eligible.art_dose_End >= REPORT_END_DATE
  AND art_start_date <= REPORT_END_DATE;