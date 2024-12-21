WITH FollowUp AS (SELECT follow_up.encounter_id,
                         follow_up.client_id,
                         date_of_event                       as hiv_confirmed_date,
                         art_antiretroviral_start_date       as art_start_date,
                         follow_up_date_followup_            as followup_date,
                         weight_text_                        as weight_in_kg,
                         pregnancy_status,
                         regimen,
                         antiretroviral_art_dispensed_dose_i as art_dose_days,
                         follow_up_status,
                         anitiretroviral_adherence_level,
                         next_visit_date,
                         dsd_category,
                         date_started_on_tuberculosis_prophy as tpt_start_date,
                         date_completed_tuberculosis_prophyl as tpt_completed_date,
                         date_discontinued_tuberculosis_prop as tpt_discontinued_date,
                         date_viral_load_results_received    as viral_load_performed_date,
                         viral_load_test_status,
                         treatment_end_date                  as art_end_date,
                         current_who_hiv_stage,
                         cd4_count,
                         cotrimoxazole_prophylaxis_start_dat,
                         cotrimoxazole_prophylaxis_stop_date,
                         patient_diagnosed_with_active_tuber as active_tb_dx,
                         diagnosis_date,
                         tuberculosis_drug_treatment_start_d,
                         date_active_tbrx_completed,
                         tb_prophylaxis_type                 AS TB_ProphylaxisType,
                         tb_prophylaxis_type_alternate_      AS TB_ProphylaxisTypeALT,
                         tpt_followup_6h_                       tpt_follow_up_inh,
                         date_started_on_tuberculosis_prophy AS inhprophylaxis_started_date,
                         date_completed_tuberculosis_prophyl AS InhprophylaxisCompletedDate,
                         why_eligible_reason_,
                         tb_diagnostic_test_result              tb_specimen_type,
                         fluconazole_start_date              AS Fluconazole_Start_Date,
                         fluconazole_stop_date               as Fluconazole_End_Date,
                         transfer_in
                  FROM mamba_flat_encounter_follow_up follow_up
                           join mamba_flat_encounter_follow_up_1 follow_up_1
                                on follow_up.encounter_id = follow_up_1.encounter_id
                           LEFT join mamba_flat_encounter_follow_up_2 follow_up_2
                                on follow_up.encounter_id = follow_up_2.encounter_id
                           LEFT join mamba_flat_encounter_follow_up_3 follow_up_3
                                on follow_up.encounter_id = follow_up_3.encounter_id),
     tmp_tpt_type as (SELECT encounter_id,
                             client_id,
                             TB_ProphylaxisType                                                                                                     AS TptType,
                             TB_ProphylaxisTypeAlt                                                                                                  AS TptTypeAlt,
                             tpt_follow_up_inh                                                                                                      As TPTFollowup,
                             followup_date                                                                                                          AS FollowupDate,
                             ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.followup_date DESC , FollowUp.encounter_id DESC ) AS row_num
                      FROM FollowUp
                      where followup_date <= REPORT_END_DATE
                        and (TB_ProphylaxisType is not null OR TB_ProphylaxisTypeAlt is not null OR
                             tpt_follow_up_inh is not null)),

     tmp_tpt_start as (select encounter_id,
                              client_id,
                              inhprophylaxis_started_date                                                                                                          as inhprophylaxis_started_date,
                              ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.inhprophylaxis_started_date DESC , FollowUp.encounter_id DESC ) AS row_num
                       from FollowUp
                       where inhprophylaxis_started_date is not null
                         and followup_date <= REPORT_END_DATE),
     tmp_tpt_completed as (select encounter_id,
                                  client_id,
                                  InhprophylaxisCompletedDate                                                                                                          as InhprophylaxisCompletedDate,
                                  ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.InhprophylaxisCompletedDate DESC , FollowUp.encounter_id DESC ) AS row_num
                           from FollowUp
                           where InhprophylaxisCompletedDate is not null
                             and followup_date <= REPORT_END_DATE),

     tmp_latest_follow_up as (SELECT encounter_id,
                                     client_id,
                                     followup_date                                                                                                         AS FollowupDate,
                                     ROW_NUMBER() OVER (PARTITION BY FollowUp.client_id ORDER BY FollowUp.followup_date DESC, FollowUp.encounter_id DESC ) AS row_num
                              FROM FollowUp
                              WHERE follow_up_status IS NOT NULL
                                AND followup_date <= REPORT_END_DATE),
     tpt_type as (select * from tmp_tpt_type where row_num = 1),
     tpt_start as (select * from tmp_tpt_start where row_num = 1),
     tpt_completed as (select * from tmp_tpt_completed where row_num = 1),
     latest_follow_up as (select * from tmp_latest_follow_up where row_num = 1),
     tmp_tpt as (SELECT f_case.encounter_id,
                        f_case.client_id,
                        CASE Sex
                            WHEN 'FEMALE' THEN 'F'
                            WHEN 'MALE' THEN 'M'
                            end                             as Sex,
                        f_case.weight_in_kg,
                        client.current_age,
                        f_case.hiv_confirmed_date,
                        f_case.art_start_date,
                        f_case.followup_date,
                        f_case.why_eligible_reason_,
                        art_dose_days                       as artdosecode,
                        f_case.next_visit_date,
                        f_case.follow_up_status,
                        f_case.follow_up_status             as statuscode,
                        f_case.art_end_date,
                        f_case.current_who_hiv_stage        AS WHOStage,
                        cd4_count                              AdultCD4Count,
                        cd4_count                              ChildCD4Count,
                        cotrimoxazole_prophylaxis_start_dat As CPT_StartDate,
                        cotrimoxazole_prophylaxis_start_dat As CPT_StartDate_GC,
                        cotrimoxazole_prophylaxis_stop_date As CPT_StopDate,
                        cotrimoxazole_prophylaxis_stop_date As CPT_StopDate_GC,
                        tb_specimen_type                    AS TB_SpecimenType,
                        active_tb_dx                        As ActiveTBDiagnosed,
                        diagnosis_date                      As ActiveTBDignosedDate,
                        diagnosis_date                      As ActiveTBDignosedDate_GC,
                        tuberculosis_drug_treatment_start_d As TBTx_StartDate,
                        tuberculosis_drug_treatment_start_d As TBTx_StartDate_GC,
                        date_active_tbrx_completed          As TBTx_CompletedDate,
                        date_active_tbrx_completed          As TBTx_CompletedDate_GC,
                        client.patient_uuid                 as PatientGUID
                 FROM FollowUp AS f_case
                          INNER JOIN latest_follow_up ON f_case.encounter_id = latest_follow_up.encounter_id
                          LEFT JOIN mamba_dim_client client on latest_follow_up.client_id = client.client_id)

select tmp_tpt.Sex,
       tmp_tpt.weight_in_kg                                                as Weight,
       TIMESTAMPDIFF(YEAR, client.date_of_birth, REPORT_END_DATE)                 as Age,
       tpt_start.inhprophylaxis_started_date                               as TPT_Started_Date,
       tpt_completed.InhprophylaxisCompletedDate                           as TPT_Completed_Date,
       tpt_type.TptType                                                    as TPT_Type,
       tpt_type.TptTypeAlt                                                    TPT_TypeAlt,
       CASE
           WHEN tpt_type.TptType = '6H' THEN 'INH'
           WHEN tpt_type.TptType = '3HP' THEN '3HP'
           ELSE '' END                                                     AS TPT_TypeChar,
       tmp_tpt.hiv_confirmed_date                                          as HIV_Confirmed_Date,
       tmp_tpt.art_start_date                                              as ART_Start_Date,
       tmp_tpt.followup_date                                               as FollowUpDate,
       Transfer_In,
       tmp_tpt.artdosecode                                                 as ARTDoseDays,
       tmp_tpt.next_visit_date                                             as Next_visit_Date,
       CASE
           WHEN tmp_tpt.follow_up_status = 'Transferred out' THEN 0
           WHEN tmp_tpt.follow_up_status = 'Stop all' THEN 1
           WHEN tmp_tpt.follow_up_status = 'Loss to follow-up (LTFU)' THEN 2
           WHEN tmp_tpt.follow_up_status = 'Ran away' THEN 3
           WHEN tmp_tpt.follow_up_status = 'Dead' THEN 4
           WHEN tmp_tpt.follow_up_status = 'Alive' THEN 5
           WHEN tmp_tpt.follow_up_status = 'Restart medication' THEN 6 END
                                                                           as FollowupStatus,
       tmp_tpt.follow_up_status                                            as FollowupStatusChar,
       tmp_tpt.art_end_date                                                as ARTDoseEndDate,
       tmp_tpt.PatientGUID                                                 as PatientGUID,
       tmp_tpt.WHOStage                                                    as WHOStage,
       AdultCD4Count,
       ChildCD4Count,
       fn_gregorian_to_ethiopian_calendar(CPT_StartDate, 'D/M/Y')          as CPT_StartDate,
       CPT_StartDate_GC,
       fn_gregorian_to_ethiopian_calendar(CPT_StopDate, 'D/M/Y')           as CPT_StopDate,
       CPT_StopDate_GC,
       TB_SpecimenType,
       ActiveTBDiagnosed,
       fn_gregorian_to_ethiopian_calendar(ActiveTBDignosedDate, 'D/M/Y')   as ActiveTBDignosedDate,
       ActiveTBDignosedDate_GC,
       fn_gregorian_to_ethiopian_calendar(TBTx_StartDate, 'D/M/Y')         as TBTx_StartDate,
       TBTx_StartDate_GC,
       fn_gregorian_to_ethiopian_calendar(TBTx_CompletedDate, 'D/M/Y')     as TBTx_CompletedDate,
       TBTx_CompletedDate_GC,
       fn_gregorian_to_ethiopian_calendar(Fluconazole_Start_Date, 'D/M/Y') as FluconazoleStartDate,
       Fluconazole_Start_Date                                              as FluconazoleStartDate_GC,
       fn_gregorian_to_ethiopian_calendar(Fluconazole_End_Date, 'D/M/Y')   as FluconazoleEndDate,
       Fluconazole_End_Date                                                as FluconazoleEndDate_GC

FROM FollowUp
         inner join tmp_tpt on tmp_tpt.encounter_id = FollowUp.encounter_id
         Left join tpt_start on tmp_tpt.client_id = tpt_start.client_id
         Left join tpt_completed on tmp_tpt.client_id = tpt_completed.client_id
         Left join tpt_type on tmp_tpt.client_id = tpt_type.client_id
         left join mamba_dim_client client on tmp_tpt.client_id = client.client_id
where tmp_tpt.art_end_date >= REPORT_END_DATE
  AND tmp_tpt.follow_up_status in ('Alive', 'Restart medication')
  AND tmp_tpt.art_start_date <= REPORT_END_DATE and  TIMESTAMPDIFF(MONTH, tmp_tpt.art_start_date, REPORT_END_DATE) > 0;