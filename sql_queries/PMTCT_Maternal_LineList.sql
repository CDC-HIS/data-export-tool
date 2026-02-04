WITH Enrollment AS (SELECT client_id,
                           encounter_id,
                           antenatal_care_provider,
                           ld_client,
                           post_natal_care,
                           art_clinic,
                           location_of_birth,
                           date_of_enrollment_or_booking,
                           currently_breastfeeding_child,
                           pregnancy_status,
                           date_referred_to_pmtct,
                           ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY date_of_enrollment_or_booking , encounter_id ) as row_num
                    FROM mamba_flat_encounter_pmtct_enrollment
    --   WHERE date_of_enrollment_or_booking BETWEEN REPORT_START_DATE AND REPORT_END_DATE
),

     Discharge AS (SELECT *,
                          ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY discharge_date , encounter_id ) as row_num
                   FROM mamba_flat_encounter_pmtct_discharge
         -- WHERE discharge_date > REPORT_START_DATE
     ),

     Episode_Window AS (SELECT e.client_id,
                               e.encounter_id                              as enrollment_id,
                               e.antenatal_care_provider,
                               e.ld_client,
                               e.post_natal_care,
                               e.art_clinic,
                               e.date_of_enrollment_or_booking             as start_date,
                               d.discharge_date,
                               d.reason_for_discharge_from_pmtct,
                               d.reason_for_discharge_from_pmtct           as discharge_outcome,
                               e.currently_breastfeeding_child,
                               e.pregnancy_status,
                               e.date_referred_to_pmtct,
                               COALESCE(d.discharge_date, REPORT_END_DATE) as effective_end_date
                        FROM Enrollment e
                                 LEFT JOIN Discharge d
                                           ON e.client_id = d.client_id
                                               AND e.row_num = d.row_num
                                               AND d.discharge_date > e.date_of_enrollment_or_booking),

     FollowUp AS (SELECT follow_up.encounter_id,
                         follow_up.client_id                 AS PatientId,
                         follow_up_status,
                         follow_up_date_followup_            AS follow_up_date,
                         art_antiretroviral_start_date       AS art_start_date,
                         antiretroviral_art_dispensed_dose_i AS ARTDoseDays,
                         anitiretroviral_adherence_level     AS AdherenceLevel,
                         regimen,
                         currently_breastfeeding_child       as breast_feeding_status,
                         pregnancy_status,
                         nutritional_status_of_adult,
                         hiv_viral_load                      AS viral_load_count,
                         cd4_count,
                         date_of_reported_hiv_viral_load     as viral_load_sent_date,
                         date_viral_load_results_received    AS viral_load_perform_date,
                         date_referred_to_pmtct,
                         dsd_category,
                         next_visit_date,
                         weight_text_,
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
                         )                                   AS routine_viral_load_test_indication,
                         COALESCE(repeat_or_confirmatory_vl_initial_viral_load_greater_than_10,
                                  suspected_antiretroviral_failure
                         )                                   AS targeted_viral_load_test_indication,
                         viral_load_test_status
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

     Events_Ranked AS (SELECT ew.enrollment_id,
                              f.*,

                              ROW_NUMBER() OVER (
                                  PARTITION BY ew.enrollment_id
                                  ORDER BY CASE
                                               WHEN f.follow_up_date BETWEEN ew.start_date AND ew.effective_end_date
                                                   THEN f.follow_up_date
                                               ELSE NULL
                                      END DESC,
                                      f.encounter_id DESC
                                  ) as rn_latest_visit, ROW_NUMBER() OVER (
                                  PARTITION BY ew.enrollment_id
                                  ORDER BY CASE
                                               WHEN f.viral_load_sent_date BETWEEN ew.start_date AND ew.effective_end_date
                                                   THEN f.viral_load_sent_date
                                               ELSE NULL
                                      END DESC
                                  ) as rn_latest_vl_sent, ROW_NUMBER() OVER (
                                  PARTITION BY ew.enrollment_id
                                  ORDER BY CASE
                                               WHEN f.viral_load_perform_date BETWEEN ew.start_date AND ew.effective_end_date
                                                   THEN f.viral_load_perform_date
                                               ELSE NULL
                                      END DESC
                                  ) as rn_latest_vl_res, ROW_NUMBER() OVER (
                                  PARTITION BY ew.enrollment_id
                                  ORDER BY CASE
                                               WHEN f.dsd_category IS NOT NULL
                                                   AND f.follow_up_date BETWEEN ew.start_date AND ew.effective_end_date
                                                   THEN f.follow_up_date
                                               ELSE NULL
                                      END DESC,
                                      f.encounter_id DESC
                                  ) as rn_latest_dsd
                       FROM Episode_Window ew
                                LEFT JOIN FollowUp f
                                          ON ew.client_id = f.PatientId)
SELECT client.patient_uuid                                           PatientGUID,
       client.Sex,
       TIMESTAMPDIFF(YEAR, client.date_of_birth, REPORT_END_DATE) as `Age`,
       visit.weight_text_                                         as Weight,
       ew.start_date                                              as BookingDate,
       visit.art_start_date                                       AS ArtStartDate,
       ew.start_date                                              as BookingDate,
       COALESCE(ew.art_clinic, ew.antenatal_care_provider, ew.ld_client,
                ew.post_natal_care)                               as StatusatEnrollment,
       ew.date_referred_to_pmtct                                  AS DateReferredtoPMTCT,
       ew.pregnancy_status                                        AS IsPregnant,
       ew.currently_breastfeeding_child                           AS IsBreastfeeding,
       ew.discharge_date                                          AS DateofDischarge,
       ew.reason_for_discharge_from_pmtct                            ReasonforDischarge,
       ew.discharge_outcome                                       AS MaternalPMTCTFinalOutcome,
       visit.follow_up_date                                       AS LatestFollowupDate,
       visit.follow_up_status                                     AS LatestFollowupStatus,
       visit.regimen                                              as Regimen,
       visit.ARTDoseDays                                          as Dose,
       visit.nutritional_status_of_adult                          As NutritionalStatus,
       visit.AdherenceLevel                                       AS Adherence,
       vl_s.viral_load_sent_date                                  AS Viral_Load_Sent_Date,
       COALESCE(vl_s.routine_viral_load_test_indication,
                vl_s.targeted_viral_load_test_indication)         AS Viral_Load_Indication,
       vl_r.viral_load_perform_date                               as Viral_Load_Received_Date,
       vl_r.viral_load_test_status                                AS LatestVLStatus,
       vl_s.cd4_count                                             as CD4Count,
       visit.next_visit_date                                      AS NextVisitDate
FROM Episode_Window ew
         LEFT JOIN mamba_dim_client client ON ew.client_id = client.client_id
         LEFT JOIN Events_Ranked visit
                   ON ew.enrollment_id = visit.enrollment_id AND visit.rn_latest_visit = 1
         LEFT JOIN Events_Ranked vl_s
                   ON ew.enrollment_id = vl_s.enrollment_id AND vl_s.rn_latest_vl_sent = 1
         LEFT JOIN Events_Ranked vl_r
                   ON ew.enrollment_id = vl_r.enrollment_id AND vl_r.rn_latest_vl_res = 1
         LEFT JOIN Events_Ranked dsd
                   ON ew.enrollment_id = dsd.enrollment_id AND dsd.rn_latest_dsd = 1;