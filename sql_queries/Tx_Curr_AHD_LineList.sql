WITH FollowUp AS (select follow_up.encounter_id,
                         follow_up.client_id                 AS PatientId,
                         follow_up_status,
                         follow_up_date_followup_            AS follow_up_date,
                         art_antiretroviral_start_date       AS art_start_date,
                         date_started_on_tuberculosis_prophy AS inhprophylaxis_started_date,
                         date_completed_tuberculosis_prophyl AS InhprophylaxisCompletedDate,
                         tb_prophylaxis_type                 AS TB_ProphylaxisType,
                         tpt_dispensed_dose_in_days_alternat AS TPT_DoseDaysNumberALT, -- ???
                         tpt_side_effects                    AS TPT_SideEffect,
                         diagnostic_test                     AS DiagnosticTest,
                         tb_diagnostic_test_result           AS DiagnosticTestResult,
                         lf_lam_result                       AS LF_LAM_result,
                         gene_xpert_result                   AS Gene_Xpert_result,
                         tuberculosis_drug_treatment_start_d AS activetbtreatmentStartDate,
                         tpt_dispensed_dose_in_days_inh_     AS TPT_DoseDaysNumberINH,
                         was_the_patient_screened_for_tuberc AS tb_screened,
                         tb_screening_date                   AS tb_screening,
                         Adherence                           AS TPT_Adherance,
                         date_discontinued_tuberculosis_prop AS inhprophylaxisdiscontinuedDate,
                         date_active_tbrx_completed          AS ActiveTBTreatmentCompletedDate,
                         date_active_tbrx_dc                 AS activetbtreatmentDisContinuedDate,
                         cervical_cancer_screening_status    AS CCS_ScreenDoneYes,
                         date_of_reported_hiv_viral_load     AS viral_load_sent_date,
                         date_viral_load_results_received    AS viral_load_perform_date,
                         viral_load_test_status,
                         hiv_viral_load                      AS viral_load_count,
                         viral_load_test_indication,
                         treatment_end_date,
                         weight_text_                        AS Weight,
                         height,
                         date_of_event                       AS date_hiv_confirmed,
                         current_who_hiv_stage,
                         cd4_count,
                         antiretroviral_art_dispensed_dose_i AS art_dose_days,
                         regimen,
                         adherence,
                         pregnancy_status,
                         method_of_family_planning,
                         crag,
                         cotrimoxazole_prophylaxis_start_dat,
                         cotrimoxazole_prophylaxis_stop_date,
                         current_functional_status,
                         patient_diagnosed_with_active_tuber,
                         fluconazole_start_date              AS Fluconazole_Start_Date,
                         weight_for_age_status               AS NSLessthanFive,
                         nutritional_status_of_older_child_a AS NSAdolescent,
                         nutritional_status_of_adult         AS ns_adult,
                         no_opportunistic_illness            AS No_OI,
                         herpes_zoster                       AS Zoster,
                         bacterial_pneumonia                 AS Bacterial_Pneumonia,
                         extra_pulmonary_tuberculosis_tb     AS Extra_Pulmonary_TB,
                         candidiasis_of_the_esophagus        AS Oesophageal_Candidiasis,
                         candidiasis_vaginal                 AS Vaginal_Candidiasis,
                         mouth_ulcer                         AS Mouth_Ulcer,
                         diarrhea_chronic                    AS Chronic_Diarrhea,
                         acute_diarrhea                      AS Acute_Diarrhea,
                         toxoplasmosis                       AS CNS_Toxoplasmosis,
                         meningitis_cryptococcal             AS Cryptococcal_Meningitis,
                         kaposi_sarcoma_oral                 AS Kaposi_Sarcoma,
                         suspected_cervical_cancer           AS Cervical_Cancer,
                         pulmonary_tuberculosis_tb           AS Pulmonary_TB,
                         candidiasis_oral                    AS Oral_Candidiasis,
                         pneumocystis_carinii_pneumonia_pcp  AS Pneumocystis_Pneumonia,
                         malignant_lymphoma_nonhodgkins      AS NonHodgkins_Lymphoma,
                         female_genital_ulcer_disease        AS Genital_Ulcer,
                         other_opportunistic_illnesses       AS OI_Other,
                         fluconazole_stop_date               as Fluconazole_End_Date,
                         nutritional_screening_result,
                         dsd_category,
                         other_medications_med_1                Med1,
                         other_medications_med2                 Med2
                  FROM mamba_flat_encounter_follow_up follow_up
                           JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                ON follow_up.encounter_id = follow_up_1.encounter_id
                           JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                ON follow_up.encounter_id = follow_up_2.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                     ON follow_up.encounter_id = follow_up_3.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_4 follow_up_4
                                     ON follow_up.encounter_id = follow_up_4.encounter_id),
     tmp_tpt_start AS (SELECT patientid,
                              inhprophylaxis_started_date AS inhprophylaxis_started_date,
                              ROW_NUMBER()                   OVER (PARTITION BY PatientId ORDER BY inhprophylaxis_started_date DESC, encounter_id DESC) AS row_num
                       FROM FollowUp
                       WHERE inhprophylaxis_started_date IS NOT NULL),
     tpt_start as (select * from tmp_tpt_start where row_num = 1),

     tmp_tpt_completed AS (SELECT patientid,
                                  InhprophylaxisCompletedDate AS InhprophylaxisCompletedDate,
                                  ROW_NUMBER()                   OVER (PARTITION BY PatientId ORDER BY InhprophylaxisCompletedDate DESC, encounter_id DESC) AS row_num
                           FROM FollowUp
                           WHERE InhprophylaxisCompletedDate IS NOT NULL),
     tpt_completed as (select *
                       from tmp_tpt_completed
                       where row_num = 1),

     tmp_tpt_type AS (SELECT patientid,
                             TB_ProphylaxisType AS TB_ProphylaxisType,
                             ROW_NUMBER()          OVER (PARTITION BY PatientId ORDER BY TB_ProphylaxisType DESC, encounter_id DESC) AS row_num
                      FROM FollowUp
                      WHERE TB_ProphylaxisType IS NOT NULL),
     tpt_type as (select * from tmp_tpt_type where row_num = 1),

     tmp_tpt_dose_ALT AS (SELECT patientid,
                                 TPT_DoseDaysNumberALT AS TPT_DoseDaysNumberALT,
                                 ROW_NUMBER()             OVER (PARTITION BY PatientId ORDER BY TPT_DoseDaysNumberALT DESC, encounter_id DESC) AS row_num
                          FROM FollowUp
                          WHERE TPT_DoseDaysNumberALT IS NOT NULL),
     tpt_dose_ALT as (select * from tmp_tpt_dose_ALT where row_num = 1),

     tmp_tpt_dose_INH AS (SELECT patientid,
                                 TPT_DoseDaysNumberINH AS TPT_DoseDaysNumberINH,
                                 ROW_NUMBER()             OVER (PARTITION BY PatientId ORDER BY TPT_DoseDaysNumberINH DESC, encounter_id DESC) AS row_num
                          FROM FollowUp
                          WHERE TPT_DoseDaysNumberINH IS NOT NULL),
     tpt_dose_INH as (select * from tmp_tpt_dose_INH where row_num = 1),

     tmp_tpt_side_effect AS (SELECT patientid,
                                    TPT_SideEffect AS TPT_SideEffect,
                                    ROW_NUMBER()      OVER (PARTITION BY PatientId ORDER BY TPT_SideEffect DESC, encounter_id DESC) AS row_num
                             FROM FollowUp
                             WHERE TPT_SideEffect IS NOT NULL),
     tpt_side_effect as (select * from tmp_tpt_side_effect where row_num = 1),

     tmp_tb_diagnostic_test AS (SELECT patientid,
                                       DiagnosticTest AS TB_Diagnostic_Test,
                                       ROW_NUMBER()      OVER (PARTITION BY PatientId ORDER BY DiagnosticTest DESC, encounter_id DESC) AS row_num
                                FROM FollowUp
                                WHERE DiagnosticTest IS NOT NULL),
     tb_diagnostic_test as (select * from tmp_tb_diagnostic_test where row_num = 1),

     tmp_tb_diagnostic_result AS (SELECT patientid,
                                         DiagnosticTestResult AS TB_Diagnostic_Result,
                                         ROW_NUMBER()            OVER (PARTITION BY PatientId ORDER BY DiagnosticTestResult DESC, encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  WHERE DiagnosticTestResult IS NOT NULL),
     tb_diagnostic_result as (select * from tmp_tb_diagnostic_result where row_num = 1),

     tmp_tb_LF_LAM_result AS (SELECT patientid,
                                     LF_LAM_result AS LF_LAM_result,
                                     ROW_NUMBER()     OVER (PARTITION BY PatientId ORDER BY LF_LAM_result DESC, encounter_id DESC) AS row_num
                              FROM FollowUp
                              WHERE LF_LAM_result IS NOT NULL),
     tb_LF_LAM_result as (select * from tmp_tb_LF_LAM_result where row_num = 1),
     tmp_tb_Gene_Xpert_result AS (SELECT patientid,
                                         Gene_Xpert_result AS Gene_Xpert_result,
                                         ROW_NUMBER()         OVER (PARTITION BY PatientId ORDER BY Gene_Xpert_result DESC, encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  WHERE Gene_Xpert_result IS NOT NULL),
     tb_Gene_Xpert_result as (select * from tmp_tb_Gene_Xpert_result where row_num = 1),

     tmp_tpt_screened AS (SELECT patientid,
                                 tb_screened AS TB_Screened,
                                 ROW_NUMBER()   OVER (PARTITION BY PatientId ORDER BY tb_screened DESC, encounter_id DESC) AS row_num
                          FROM FollowUp
                          WHERE tb_screened IS NOT NULL),
     tpt_screened as (select * from tmp_tpt_screened where row_num = 1),
     tmp_tpt_screening AS (SELECT patientid,
                                  tb_screening AS TB_Screening_Result,
                                  ROW_NUMBER()    OVER (PARTITION BY PatientId ORDER BY tb_screening DESC, encounter_id DESC) AS row_num
                           FROM FollowUp
                           WHERE tb_screening IS NOT NULL),
     tpt_screening as (select * from tmp_tpt_screening where row_num = 1),
     tmp_tpt_adherence AS (SELECT patientid,
                                  TPT_Adherance AS TPT_Adherence,
                                  ROW_NUMBER()     OVER (PARTITION BY PatientId ORDER BY TPT_Adherance DESC, encounter_id DESC) AS row_num
                           FROM FollowUp
                           WHERE TPT_Adherance IS NOT NULL),
     tpt_adherence as (select * from tmp_tpt_adherence where row_num = 1),
     tmp_ActiveTBTreatmentStarted AS (SELECT patientid,
                                             activetbtreatmentStartDate AS ActiveTBTreatmentStartDate,
                                             ROW_NUMBER()                  OVER (PARTITION BY PatientId ORDER BY activetbtreatmentStartDate DESC, encounter_id DESC) AS row_num
                                      FROM FollowUp
                                      WHERE activetbtreatmentStartDate IS NOT NULL),
     ActiveTBTreatmentStarted as (select * from tmp_ActiveTBTreatmentStarted where row_num = 1),
     tmp_TBTreatmentCompleted AS (SELECT patientid,
                                         ActiveTBTreatmentCompletedDate AS ActiveTBTreatmentCompletedDate,
                                         ROW_NUMBER()                      OVER (PARTITION BY PatientId ORDER BY ActiveTBTreatmentCompletedDate DESC, encounter_id DESC) AS row_num
                                  FROM FollowUp
                                  WHERE ActiveTBTreatmentCompletedDate IS NOT NULL),
     TBTreatmentCompleted as (select * from tmp_TBTreatmentCompleted where row_num = 1),
     tmp_TBTreatmentDiscontinued AS (SELECT patientid,
                                            activetbtreatmentDisContinuedDate AS ActiveTBTreatmentDiscontinuedDate,
                                            ROW_NUMBER()                         OVER (PARTITION BY PatientId ORDER BY activetbtreatmentDisContinuedDate DESC, encounter_id DESC) AS row_num
                                     FROM FollowUp
                                     WHERE activetbtreatmentDisContinuedDate IS NOT NULL),
     TBTreatmentDiscontinued as (select * from tmp_TBTreatmentDiscontinued where row_num = 1),
     tmp_cca_screened_tmp AS (SELECT DISTINCT patientid,
                                              CCS_ScreenDoneYes AS CCA_Screened,
                                              ROW_NUMBER()         OVER (PARTITION BY PatientId ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                              FROM FollowUp
                              where CCS_ScreenDoneYes IS NOT NULL),
     cca_screened AS (select * from tmp_cca_screened_tmp where row_num = 1),
-- VL Sent Date
     tmp_vl_sent_date AS (select PatientId,
                                 encounter_id,
                                 viral_load_sent_date,
                                 ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY viral_load_sent_date DESC, encounter_id DESC) AS row_num
                          from FollowUp
                          where follow_up_date <= REPORT_END_DATE),
     vl_sent_date AS (select * from tmp_vl_sent_date where row_num = 1),
-- VL Performed date

     vl_performed_date_tmp AS (SELECT FollowUp.encounter_id,
                                      FollowUp.PatientId,
                                      FollowUp.viral_load_perform_date,
                                      FollowUp.viral_load_test_status,
                                      FollowUp.viral_load_count AS viral_load_count,
                                      CASE
                                          WHEN vl_sent_date.viral_load_sent_date IS NOT NULL
                                              THEN vl_sent_date.viral_load_sent_date
                                          WHEN FollowUp.viral_load_perform_date IS NOT NULL
                                              THEN FollowUp.viral_load_perform_date
                                          ELSE NULL END         AS viral_load_ref_date,
                                      ROW_NUMBER()                 OVER (PARTITION BY FollowUp.PatientId ORDER BY viral_load_perform_date DESC, FollowUp.encounter_id DESC) AS row_num
                               FROM FollowUp
                                        LEFT JOIN vl_sent_date ON FollowUp.PatientId = vl_sent_date.PatientId
                               WHERE follow_up_status IS NOT NULL
                                 AND art_start_date IS NOT NULL
                                 AND viral_load_perform_date <= REPORT_END_DATE),
     vl_performed_date AS (select * from vl_performed_date_tmp where row_num = 1),
     tx_curr_all AS (SELECT PatientId,
                            follow_up_date AS FollowupDate,
                            encounter_id,
                            ROW_NUMBER()      OVER (PARTITION BY PatientId ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                     FROM FollowUp
                     WHERE follow_up_status IS NOT NULL
                       AND art_start_date IS NOT NULL
                       AND follow_up_date <= REPORT_END_DATE),
     tx_curr AS (select * from tx_curr_all where row_num = 1)


SELECT DISTINCT CASE client.sex
                    WHEN 'FEMALE' THEN 'F'
                    WHEN 'MALE' THEN 'M'
                    end                                                                          as Sex,
                f_case.Weight                                                                    as Weight,
                client.current_age                                                               as Age,
                client.patient_uuid                                                              as PatientGUID,
                f_case.height                                                                    as Height,
                f_case.date_hiv_confirmed                                                        as HIV_Confirmed_Date,
                f_case.art_start_date                                                            as ARTStartDate,
                FLOOR(DATEDIFF(REPORT_END_DATE, f_case.art_start_date) / 30.4375)                as MonthsOnART,
                f_case.follow_up_date                                                            as FollowUpDate,
                f_case.current_who_hiv_stage                                                     as WHOStage,
                f_case.cd4_count                                                                 as CD4Count,
                f_case.art_dose_days                                                             as ARTDoseDays,
                f_case.regimen                                                                   as ARVRegimen,
                f_case.follow_up_status                                                          as FollowupStatus,
                tpt_adherence.tpt_adherence                                                      as AdheranceLevel,
                f_case.pregnancy_status                                                          as IsPregnant,
                f_case.method_of_family_planning                                                 as FpMethodUsed,
                f_case.crag                                                                      as CrAg,
                COALESCE(
                        f_case.ns_adult,
                        f_case.NSAdolescent,
                        f_case.NSLessthanFive
                )                                                                                as NutritionalStatus,
                f_case.current_functional_status                                                 as FunctionalStatus,
                f_case.No_OI                                                                     as No_OI,
                f_case.Zoster                                                                    as Zoster,
                f_case.Bacterial_Pneumonia                                                       as Bacterial_Pneumonia,
                f_case.Extra_Pulmonary_TB                                                        as Extra_Pulmonary_TB,
                f_case.Oesophageal_Candidiasis                                                   as Oesophageal_Candidiasis,
                f_case.Vaginal_Candidiasis                                                       as Vaginal_Candidiasis,
                f_case.Mouth_Ulcer                                                               as Mouth_Ulcer,
                f_case.Chronic_Diarrhea                                                          as Chronic_Diarrhea,
                f_case.Acute_Diarrhea                                                            as Acute_Diarrhea,
                f_case.CNS_Toxoplasmosis                                                         as CNS_Toxoplasmosis,
                f_case.Cryptococcal_Meningitis                                                   as Cryptococcal_Meningitis,
                f_case.Kaposi_Sarcoma                                                            as Kaposi_Sarcoma,
                f_case.Cervical_Cancer                                                           as Cervical_Cancer,
                f_case.Pulmonary_TB                                                              as Pulmonary_TB,
                f_case.Oral_Candidiasis                                                          as Oral_Candidiasis,
                f_case.Pneumocystis_Pneumonia                                                    as Pneumocystis_Pneumonia,
                f_case.NonHodgkins_Lymphoma                                                      as NonHodgkins_Lymphoma,
                f_case.Genital_Ulcer                                                             as Genital_Ulcer,
                f_case.OI_Other                                                                  as OI_Other,
                f_case.Med1                                                                      as Med1,
                f_case.Med2                                                                      as Med2,
                f_case.cotrimoxazole_prophylaxis_start_dat                                       as CotrimoxazoleStartDate,
                f_case.cotrimoxazole_prophylaxis_stop_date                                          cortimoxazole_stop_date,
                f_case.Fluconazole_Start_Date                                                    as Fluconazole_Start_Date,
                f_case.Fluconazole_End_Date                                                      as Fluconazole_End_Date,
                tpt_type.TB_ProphylaxisType                                                      as TPT_Type,
                tpt_start.inhprophylaxis_started_date                                            as inhprophylaxis_started_date,
                tpt_completed.InhprophylaxisCompletedDate                                        as InhprophylaxisCompletedDate,
                tpt_dose_ALT.TPT_DoseDaysNumberALT                                               as TPT_DoseDaysNumberALT,
                tpt_dose_INH.TPT_DoseDaysNumberINH                                               as TPT_DoseDaysNumberINH,
                COALESCE(tpt_dose_INH.TPT_DoseDaysNumberINH, tpt_dose_ALT.TPT_DoseDaysNumberALT) AS TPT_Dispensed_Dose,
                tpt_side_effect.TPT_SideEffect                                                   as TPT_SideEffect,
                tpt_adherence.TPT_Adherence                                                      as TPT_Adherence,
                tpt_screened.TB_Screened                                                         as tb_screened,
                tpt_screening.TB_Screening_Result                                                as tb_screening_result,
                tb_diagnostic_result.TB_Diagnostic_Result                                        as TB_Diagnostic_Result,
                tb_LF_LAM_result.LF_LAM_result                                                   as LF_LAM_result,
                tb_Gene_Xpert_result.Gene_Xpert_result                                           as Gene_Xpert_result,
                CASE
                    WHEN tb_diagnostic_test.TB_Diagnostic_Test = 'Smear microscopy only' AND
                         tb_diagnostic_result.TB_Diagnostic_Result = 'Positive'
                        THEN 'Positive'
                    WHEN tb_diagnostic_test.TB_Diagnostic_Test = 'Smear microscopy only' AND
                         tb_diagnostic_result.TB_Diagnostic_Result = 'Negative'
                        THEN 'Negative'
                    ELSE '' END                                                                  AS Smear_Microscopy_Result,
                CASE
                    WHEN tb_diagnostic_test.TB_Diagnostic_Test = 'Additional test other than Gene-Xpert' AND
                         tb_diagnostic_result.TB_Diagnostic_Result = 'Positive'
                        THEN 'Positive'
                    WHEN tb_diagnostic_test.TB_Diagnostic_Test = 'Additional test other than Gene-Xpert' AND
                         tb_diagnostic_result.TB_Diagnostic_Result = 'Negative'
                        THEN 'Negative'
                    ELSE '' END                                                                  AS Additional_TB_Diagnostic_Test_Result,
                f_case.patient_diagnosed_with_active_tuber                                       as Active_TB,
                ActiveTBTreatmentStarted.ActiveTBTreatmentStartDate                              as ActiveTBTreatmentStartDate,
                TBTreatmentCompleted.ActiveTBTreatmentCompletedDate                              as ActiveTBTreatmentCompletedDate,
                TBTreatmentDiscontinued.ActiveTBTreatmentDiscontinuedDate                        as ActiveTBTreatmentDiscontinuedDate,
                vlperfdate.viral_load_perform_date                                               as Viral_Load_Perform_Date,
                vlperfdate.viral_load_test_status                                                as Viral_Load_Status,
                vlperfdate.viral_load_count                                                      as Viral_Load_count,
                vlsentdate.viral_load_sent_date                                                  as VL_Sent_Date,
                vlperfdate.viral_load_ref_date                                                   as Viral_Load_Ref_Date,
                cca_screened.CCA_Screened                                                        as CCA_Screened,
                f_case.dsd_category                                                              as DSD_Category,
                CASE
                    WHEN client.current_age < 5 THEN 'Yes'
                    WHEN client.current_age >= 5 AND f_case.cd4_count IS NOT NULL AND
                         f_case.cd4_count < 200 THEN 'Yes'
                    WHEN client.current_age >= 5 AND f_case.current_who_hiv_stage IS NOT NULL AND
                         (f_case.current_who_hiv_stage = 'WHO stage 3 adult' Or
                          f_case.current_who_hiv_stage = 'WHO stage 3 peds' Or
                          f_case.current_who_hiv_stage = 'WHO stage 4 peds') THEN 'Yes'
                    WHEN (client.current_age >= 5 AND f_case.current_who_hiv_stage IS NOT NULL AND
                          f_case.current_who_hiv_stage = 'WHO stage 4 adult') THEN 'Yes'
                    ELSE 'No' END                                                                as AHD
FROM FollowUp AS f_case
         INNER JOIN tx_curr ON f_case.encounter_id = tx_curr.encounter_id
         LEFT JOIN mamba_dim_client client on tx_curr.PatientId = client_id
         LEFT JOIN vl_performed_date AS vlperfdate ON vlperfdate.PatientId = f_case.PatientId
         LEFT JOIN vl_sent_date AS vlsentdate ON vlsentdate.PatientId = f_case.PatientId

         LEFT JOIN tpt_start ON tpt_start.patientid = f_case.PatientId
         LEFT JOIN tpt_completed ON tpt_completed.patientid = f_case.PatientId
         LEFT JOIN tpt_type ON tpt_type.patientid = f_case.PatientId
         LEFT JOIN tpt_dose_ALT ON tpt_dose_ALT.patientid = f_case.PatientId
         LEFT JOIN tpt_dose_INH ON tpt_dose_INH.patientid = f_case.PatientId
         LEFT JOIN tpt_side_effect ON tpt_side_effect.patientid = f_case.PatientId
         LEFT JOIN
     tpt_screened ON tpt_screened.patientid = f_case.PatientId
         LEFT JOIN tpt_screening ON tpt_screening.patientid = f_case.PatientId
         LEFT JOIN tpt_adherence ON tpt_adherence.patientid = f_case.PatientId
         LEFT JOIN tb_diagnostic_result ON tb_diagnostic_result.patientid = f_case.PatientId
         LEFT JOIN tb_diagnostic_test ON tb_diagnostic_test.patientid = f_case.PatientId
         LEFT JOIN tb_LF_LAM_result ON tb_LF_LAM_result.patientid = f_case.PatientId
         LEFT JOIN tb_Gene_Xpert_result ON tb_Gene_Xpert_result.patientid = f_case.PatientId
         LEFT JOIN ActiveTBTreatmentStarted ON ActiveTBTreatmentStarted.patientid = f_case.PatientId
         LEFT JOIN TBTreatmentCompleted ON TBTreatmentCompleted.patientid = f_case.PatientId
         LEFT JOIN TBTreatmentDiscontinued ON TBTreatmentDiscontinued.patientid = f_case.PatientId
         LEFT JOIN cca_screened ON cca_screened.patientid = f_case.PatientId
WHERE f_case.treatment_end_date >= REPORT_END_DATE
  AND f_case.follow_up_status IN ('Alive', 'Restart medication');