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
                         weight_text_                     as weight
                  FROM mamba_flat_encounter_follow_up follow_up
                           JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                ON follow_up.encounter_id = follow_up_1.encounter_id
                           JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                ON follow_up.encounter_id = follow_up_2.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                ON follow_up.encounter_id = follow_up_3.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_4 follow_up_4
                                ON follow_up.encounter_id = follow_up_4.encounter_id),
     vl_performed_date_tmp AS (SELECT FollowUp.encounter_id,
                                      FollowUp.client_id,
                                      FollowUp.viral_load_perform_date,
                                      FollowUp.viral_load_test_status,
                                      CASE
                                          WHEN viral_load_count > 0 THEN CAST(viral_load_count AS DECIMAL(12, 0))
                                          END                                                                                             AS viral_load_count,
                                      CASE
                                          WHEN FollowUp.viral_load_perform_date IS NOT NULL
                                              THEN FollowUp.viral_load_perform_date
                                          END                                                                                             AS viral_load_ref_date, -- Q this should be null?
                                      routine_viral_load_test_indication,
                                      targeted_viral_load_test_indication,
                                      ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY viral_load_perform_date DESC, encounter_id DESC) AS row_num
                               FROM FollowUp
                               WHERE follow_up_status IS NOT NULL
                                 AND art_start_date IS NOT NULL
                                 AND viral_load_perform_date <= REPORT_END_DATE
                               GROUP BY client_id, encounter_id),
     latest_follow_up_tmp AS (SELECT client_id,
                                     follow_up_date                                                                             AS FollowupDate,
                                     encounter_id,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                              FROM FollowUp
                              WHERE follow_up_status IS NOT NULL
                                AND art_start_date IS NOT NULL
                                AND follow_up_date <= REPORT_END_DATE),
     latest_follow_up AS (select * from latest_follow_up_tmp where row_num = 1),
     vl_performed_date as (select * from vl_performed_date_tmp where row_num = 1),
     vl_test_received AS (SELECT current_age,
                                 arv_dispensed_dose,
                                 treatment_end_date,
                                 art_start_date,
                                 regimen,
                                 breastfeeding_status,
                                 date_hiv_confirmed,
                                 follow_up_status,
                                 follow_up_date,
                                 pregnancy_status,
                                 next_visit_date,
                                 FollowUp.client_id,
                                 patient_uuid,
                                 CASE
                                     WHEN pregnancy_status = 'Yes' THEN 'Yes'
                                     WHEN breastfeeding_status = 'Yes' THEN 'Yes'
                                     ELSE 'No' END AS PMTCT_ART,
                                 vlperfdate.viral_load_count,
                                 vlperfdate.viral_load_perform_date,
                                 vlperfdate.viral_load_ref_date,
                                 vlperfdate.viral_load_test_status,
                                 sex,
                                 FollowUp.weight,
                                 vlperfdate.routine_viral_load_test_indication,
                                 vlperfdate.targeted_viral_load_test_indication
                          FROM FollowUp
                                   INNER JOIN latest_follow_up ON latest_follow_up.encounter_id = FollowUp.encounter_id
                                   LEFT JOIN mamba_dim_client client ON latest_follow_up.client_id = client.client_id
                                   LEFT JOIN vl_performed_date as vlperfdate
                                             ON vlperfdate.client_id = FollowUp.client_id)
select CASE client.Sex
           WHEN 'FEMALE' THEN 'F'
           WHEN 'MALE' THEN 'M'
           end                as                               Sex,
       weight                 as                               Weight,
       TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE)  as Age,
       date_hiv_confirmed,
       art_start_date         as                               art_start_date,
       follow_up_date         as                               FollowUpDate,
       pregnancy_status       as                               IsPregnant,
       breastfeeding_status   as                               Breastfeeding,
       regimen                as                               ARVDispendsedDose,
       LEFT(regimen,1)        as                               ARVRegimensLine,
       arv_dispensed_dose     as                               ARTDoseDays,
       next_visit_date,
       follow_up_status,
       treatment_end_date     as                               art_dose_End,
       viral_load_perform_date,
       viral_load_test_status as                               viral_load_status,
       viral_load_count,
       viral_load_ref_date,
       CONCAT(IFNULL(routine_viral_load_test_indication, ''), ' ',
              IFNULL(targeted_viral_load_test_indication, '')) ReasonForVLTest,
       PMTCT_ART,
       vl_test_received.patient_uuid           as                               PatientGUID
from vl_test_received
left join mamba_dim_client client on vl_test_received.client_id = client.client_id
where viral_load_perform_date is not null
  And viral_load_perform_date >= DATE_ADD(REPORT_END_DATE, INTERVAL -365 DAY)
  and viral_load_perform_date <= REPORT_END_DATE;