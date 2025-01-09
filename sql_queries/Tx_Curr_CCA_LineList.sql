WITH FollowUp AS (SELECT follow_up.encounter_id,
                         follow_up.client_id,
                         follow_up_date_followup_                          as follow_up_date,
                         follow_up_status,
                         treatment_end_date                                as art_end_date,
                         hpv_dna_result_received_date,
                         date_cytology_result_received,
                         weight_text_                                      as weight,
                         next_follow_up_screening_date                     AS ccs_next_date,
                         cervical_cancer_screening_status                  AS screening_status,
                         COALESCE(positive, negative_result)               AS ccs_hpv_result,
                         COALESCE(ascus_atypical_squamous_cells_of_undetermined_significance_o,
                                  negative_result,
                                  _ascus
                         )                                                 AS cytology_result,
                         via_screening_result                              AS ccs_via_result,
                         via_done_,
                         date_visual_inspection_of_the_cervi               AS via_date,
                         treatment_start_date                              AS ccs_treat_received_date,
                         COALESCE(not_done, normal, low_grade, high_grade) AS colposcopy_exam_finding,
                         colposcopy_exam_date,
                         purpose_for_visit_cervical_screening              as screening_type,
                         cervical_cancer_screening_method_strategy         as screening_method,
                         hpv_subtype,
                         date_hpv_test_was_done,
                         cytology_sample_collection_date,
                         biopsy_sample_collected_date,
                         biopsy_result_received_date,
                         biopsy_result,
                         treatment_of_precancerous_lesions_of_the_cervix   as CCS_Precancerous_Treat,
                         confirmed_cervical_cancer_cases_bas               as CCS_Suspicious_Treat,
                         referral_or_linkage_status,
                         reason_for_referral_cacx                          as reason_for_eligibility_transfer_in,
                         date_client_served_in_the_referred_,
                         date_client_arrived_in_the_referred,
                         date_patient_referred_out,
                         prep_offered,
                         weight_text_,
                         art_antiretroviral_start_date                     as art_start_date,
                         next_visit_date,
                         regimen,
                         antiretroviral_art_dispensed_dose_i               as dose_days,
                         pre_test_counselling_for_cervical_c                  CCaCounsellingGiven,
                         ready_for_cervical_cancer_screening                  Accepted,
                         date_counseling_given,
                         date_of_event                                     as date_hiv_confirmed,
                         transferred_in_check_this_for_all_t               as transfer_in,
                         currently_breastfeeding_child,
                         pregnancy_status
                  FROM mamba_flat_encounter_follow_up follow_up
                           join mamba_flat_encounter_follow_up_1 follow_up_1
                                on follow_up.encounter_id = follow_up_1.encounter_id
                           join mamba_flat_encounter_follow_up_2 follow_up_2
                                on follow_up.encounter_id = follow_up_2.encounter_id
                           left join mamba_flat_encounter_follow_up_3 follow_up_3
                                     on follow_up.encounter_id = follow_up_3.encounter_id
                           left join mamba_flat_encounter_follow_up_4 follow_up_4
                                     on follow_up.encounter_id = follow_up_4.encounter_id),
     tmp_cca as (SELECT encounter_id,
                        client_id,
                        follow_up_date,
                        CASE WHEN CCaCounsellingGiven = 'Yes' THEN 'Yes' WHEN 'No' THEN 'No' END                   as CCS_OfferedYes,
                        CASE WHEN CCaCounsellingGiven = 'Yes' THEN NULL WHEN 'No' THEN 'Yes' END                   as CCS_OfferedNo,
                        CASE WHEN Accepted = 'Yes' THEN 'Yes' WHEN 'No' THEN 'No' END                              as CCS_AcceptedYes,
                        CASE WHEN Accepted = 'Yes' THEN NULL WHEN 'No' THEN 'Yes' END                              as CCS_AcceptedNo,
                        CASE WHEN screening_status = 'Cervical cancer screening performed' THEN 'Yes'
                             WHEN 'Cervical cancer screening not performed' THEN 'No' END                      as CCS_ScreenDoneYes,
                        CASE WHEN screening_status = 'Cervical cancer screening performed' THEN NULL
                        WHEN 'Cervical cancer screening not performed' THEN 'Yes' END                      as CCS_ScreenDoneNo,
                        CASE
                            WHEN screening_status = 'Cervical cancer screening performed' THEN follow_up_date
                            ELSE NULL end                                                                          as CCS_ScreenDone_Date,
                        screening_type                                                                             as CCS_Screen_Type,
                        screening_method                                                                           as CCS_Screen_Method,
                        CCS_HPV_Result,
                        CCS_VIA_Result,
                        CCS_Precancerous_Treat,
                        CCS_Suspicious_Treat,
                        c.CCS_Treat_Received_Date,
                        c.CCS_Next_Date,
                        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                 FROM FollowUp AS c
                 where CCaCounsellingGiven is not null
                   and follow_up_date <= REPORT_END_DATE),
     cca as (select * from tmp_cca where row_num = 1),
     tmp_latest_follow_up as (select client_id,
                                     encounter_id,
                                     follow_up_status,
                                     follow_up_date,
                                     ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                              from FollowUp
                              where follow_up_date <= REPORT_END_DATE and follow_up_status is not null),
     latest_follow_up as (select *
                          from tmp_latest_follow_up
                          where row_num = 1),

     latest_follow_up_all as (SELECT FollowUp.client_id,
                                     FollowUp.weight,
                                     FollowUp.date_hiv_confirmed,
                                     FollowUp.art_start_date,
                                     FollowUp.follow_up_date,
                                     FollowUp.reason_for_eligibility_transfer_in,
                                     transfer_in,
                                     FollowUp.dose_days        as artdosecode,
                                     FollowUp.next_visit_date,
                                     FollowUp.follow_up_status,
                                     FollowUp.follow_up_status as statuscode,
                                     FollowUp.art_end_date,
                                     currently_breastfeeding_child,
                                     pregnancy_status

                              FROM FollowUp
                                       JOIN latest_follow_up on FollowUp.encounter_id = latest_follow_up.encounter_id)
select CASE
           WHEN client.Sex = 'FEMALE' THEN 'F'
           WHEN client.sex = 'MALE' THEN 'M'
           END                                                    as Sex,
       latest_follow_up_all.weight,
       TIMESTAMPDIFF(YEAR, client.date_of_birth, REPORT_END_DATE) AS Age,
       cca.CCS_OfferedYes                                         as CCS_OfferedYes,
       cca.CCS_OfferedNo                                          as CCS_OfferedNo,
       cca.CCS_AcceptedYes                                        as CCS_AcceptedYes,
       cca.CCS_AcceptedNo                                         as CCS_AcceptedNo,
       cca.CCS_ScreenDoneYes                                      as CCS_ScreenDoneYes,
       cca.CCS_ScreenDoneNo                                       as CCS_ScreenDoneNo,
       CCS_ScreenDone_Date,
       cca.CCS_Screen_Type                                        AS CCS_Screen_Type,
       cca.CCS_Screen_Method                                      AS CCS_Screen_Method,
       cca.CCS_HPV_Result                                         AS CCS_HPV_Result,
       cca.CCS_VIA_Result                                         AS CCS_VIA_Result,
       cca.CCS_Precancerous_Treat                                 AS CCS_Precancerous_Treat,
       cca.CCS_Suspicious_Treat                                   AS CCS_Suspicious_Treat,
       CCS_Treat_Received_Date,
       CCS_Next_Date,
       latest_follow_up_all.date_hiv_confirmed,
       latest_follow_up_all.art_start_date,
       latest_follow_up_all.follow_up_date                           FollowUpDate,
       latest_follow_up_all.transfer_in                           As Transfer_In,
       latest_follow_up_all.artdosecode                           As ARTDoseDays,
       latest_follow_up_all.next_visit_date,
       latest_follow_up_all.follow_up_status,
       latest_follow_up_all.statuscode                            As FollowupStatusChar,
       latest_follow_up_all.art_end_date                          As ARTDoseEndDate,
       client.patient_uuid                                        as PatientGUID,
       currently_breastfeeding_child                              as IsBreastfeeding,
       CASE
           WHEN currently_breastfeeding_child = 'Yes' or pregnancy_status = 'Yes'
               THEN 'YES'
           else 'No' end                                          as PMTCT_ART
FROM latest_follow_up_all
         join mamba_dim_client AS client on latest_follow_up_all.client_id = client.client_id
         Left join cca on latest_follow_up_all.client_id = cca.client_id
where (art_start_date <= REPORT_END_DATE or art_start_date is null )
  AND client.Sex = 'Female';