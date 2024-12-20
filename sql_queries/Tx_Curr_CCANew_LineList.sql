WITH     FollowUP AS (SELECT follow_up.encounter_id,
                         follow_up.client_id,
                         follow_up_date_followup_                          as follow_up_date,
                         follow_up_status,
                         treatment_end_date                                as art_end_date,
                         hpv_dna_result_received_date,
                         date_cytology_result_received,
                         next_follow_up_screening_date                     AS ccs_next_date,
                         cervical_cancer_screening_status                  AS screening_status,
                         COALESCE(positive, negative_result)               AS ccs_hpv_result,
                         COALESCE(ascus_atypical_squamous_cells_of_undetermined_significance_o,
                                  negative_result,
                                  _ascus
                         )                                                 AS cytology_result,
                         via_screening_result                              AS ccs_via_result,
                         via_done_,
                         date_visual_inspection_of_the_cervi               AS date_via_result,
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
                         confirmed_cervical_cancer_cases_bas,
                         referral_or_linkage_status,
                         reason_for_referral_cacx,
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
                         ready_for_cervical_cancer_screening                  Accepted
                  FROM mamba_flat_encounter_follow_up follow_up
                           join mamba_flat_encounter_follow_up_1 follow_up_1
                                on follow_up.encounter_id = follow_up_1.encounter_id
                           join mamba_flat_encounter_follow_up_2 follow_up_2
                                on follow_up.encounter_id = follow_up_2.encounter_id
                           left join mamba_flat_encounter_follow_up_3 follow_up_3
                                on follow_up.encounter_id = follow_up_3.encounter_id
                           left join mamba_flat_encounter_follow_up_4 follow_up_4
                                on follow_up.encounter_id = follow_up_4.encounter_id
                           ),

     tmp_prev_cxca AS (select encounter_id,
                              client_id,
                              follow_up_date                                                                             As SelectedFollowUpDate,
                              ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                       from FollowUP
                       where follow_up_date <= REPORT_START_DATE
                         and screening_status = 'Cervical cancer screening performed'),
     tmp_prev_cxca_1 as (select * from tmp_prev_cxca where row_num = 1),

     prev_cxca as (select FollowUP.client_id,
                          CASE
                              WHEN (TIMESTAMPDIFF(DAY, FollowUP.HPV_DNA_RESULT_RECEIVED_DATE, REPORT_END_DATE) > 1095 AND
                                    FollowUP.ccs_hpv_result = 'Negative result')
                                  THEN 'Need Re-Screening'
                              WHEN (TIMESTAMPDIFF(DAY, FollowUP.date_via_result, REPORT_END_DATE) > 730 AND
                                    FollowUP.ccs_via_result = 'VIA negative')
                                  THEN 'Need Re-Screening'
                              WHEN (TIMESTAMPDIFF(DAY, FollowUP.date_cytology_result_received, REPORT_END_DATE) > 1095 AND
                                    FollowUP.cytology_result = 'Negative result')
                                  THEN 'Need Re-Screening'
                              WHEN (FollowUP.ccs_treat_received_date is null AND
                                    (FollowUP.colposcopy_exam_finding = 'High Grade' OR
                                     FollowUP.colposcopy_exam_finding = 'Low Grade' OR
                                     FollowUP.cytology_result = '> Ascus' OR
                                     FollowUP.ccs_via_result = 'VIA positive: eligible for cryo/thermo-coagula' OR
                                     FollowUP.ccs_via_result = 'VIA Positive: Non-Eligible for'))
                                  THEN 'Need Re-Screening'
                              WHEN TIMESTAMPDIFF(DAY, FollowUP.ccs_treat_received_date, REPORT_END_DATE) > 181 AND
                                   FollowUP.ccs_treat_received_date is not null
                                  THEN 'Need Post Tx FU Screening'
                              WHEN (TIMESTAMPDIFF(DAY, FollowUP.hpv_dna_result_received_date, REPORT_END_DATE) > 356 AND
                                    FollowUP.ccs_hpv_result = 'Positive' AND
                                    FollowUP.ccs_via_result = 'VIA negative') THEN 'Need Re-Screening'
                              WHEN (FollowUP.ccs_next_date <= REPORT_END_DATE AND FollowUP.ccs_next_date is not null)
                                  THEN 'Need Re-Screening' END         AS PrevCxCaStatus,
                          CASE
                              WHEN FollowUP.COLPOSCOPY_EXAM_DATE IS NOT NULL and
                                   FollowUP.COLPOSCOPY_EXAM_DATE is not null
                                  THEN FollowUP.COLPOSCOPY_EXAM_DATE
                              WHEN FollowUP.date_cytology_result_received IS NOT NULL and
                                   FollowUP.date_cytology_result_received is not null
                                  THEN FollowUP.date_cytology_result_received
                              WHEN FollowUP.DATE_VIA_RESULT IS NOT NULL and FollowUP.DATE_VIA_RESULT is not null
                                  THEN FollowUP.DATE_VIA_RESULT
                              WHEN (FollowUP.HPV_DNA_RESULT_RECEIVED_DATE IS NOT NULL and
                                    FollowUP.HPV_DNA_RESULT_RECEIVED_DATE is not null) AND
                                   FollowUP.CCS_HPV_Result = 'Negative result'
                                  THEN FollowUP.HPV_DNA_RESULT_RECEIVED_DATE
                              WHEN FollowUP.follow_up_date is not null and
                                   FollowUP.screening_status = 'Cervical cancer screening performed'
                                  THEN FollowUP.follow_up_date END     AS Prev_CSS_Screen_Done_Date_Calculated,
                          FollowUP.screening_type                      as Prev_Screen_Type,
                          FollowUP.screening_method                    as Prev_Screen_Method,
                          FollowUP.HPV_SUBTYPE                         as Prev_HPV_SubType,
                          FollowUP.date_hpv_test_was_done              As Prev_HPV_DAN_SampleCollected_Date,
                          FollowUP.HPV_DNA_RESULT_RECEIVED_DATE        As Prev_HPV_DAN_ResultReceived_Date,

                          FollowUP.CCS_HPV_Result                      As Prev_HPV_Result,
                          FollowUP.DATE_VIA_RESULT                     As Prev_VIA_Screening_Date,


                          FollowUP.CCS_VIA_Result                      As Prev_VIA_Screening_Result,
                          FollowUP.CYTOLOGY_SAMPLE_COLLECTION_DATE     As Prev_Cytology_SampleCollected_Date,
                          FollowUP.date_cytology_result_received       As Prev_Cytology_ResultReceived_Date,
                          FollowUP.CYTOLOGY_RESULT                     As Prev_Cytology_Result,
                          FollowUP.COLPOSCOPY_EXAM_DATE                As Prev_Colposcopy_Exam_Date,
                          FollowUP.COLPOSCOPY_EXAM_FINDING             As Prev_Colposcopy_Exam_Result,
                          FollowUP.BIOPSY_SAMPLE_COLLECTED_DATE        As Prev_Biopsy_SampleCollected_Date,
                          FollowUP.BIOPSY_RESULT_RECEIVED_DATE         As Prev_Biopsy_ResultReceived_Date,
                          FollowUP.BIOPSY_RESULT                       As Prev_Biopsy_Result,
                          FollowUP.CCS_Precancerous_Treat              As Prev_TX_Received_for_PrecancerousLesion,

                          FollowUP.confirmed_cervical_cancer_cases_bas As Prev_TX_for_ConfirmedCxCaBasedOn_Biopsy,
                          FollowUP.CCS_Treat_Received_Date             As Prev_Date_TX_Given,
                          FollowUP.REFERRAL_OR_LINKAGE_STATUS          as Prev_ReferralStatus,

                          FollowUP.reason_for_referral_cacx            As Prev_Reason_for_Referral,
                          FollowUP.date_patient_referred_out           As Prev_Date_Referred_to_OtherHF,
                          FollowUP.date_client_arrived_in_the_referred As
                                                                          Prev_Date_Client_Arrived_in_RefferedHF,
                          FollowUP.date_client_served_in_the_referred_ AS Prev_Date_Client_Served_in_RefferedHF,
                          ccs_next_date,
                          screening_status,
                          CASE
                              WHEN FollowUP.screening_type = 'Initial visit' AND (FollowUP.CCS_VIA_Result is null) AND
                                   (FollowUP.CYTOLOGY_RESULT is null) AND
                                   (FollowUP.COLPOSCOPY_EXAM_FINDING is null) And
                                   (FollowUP.COLPOSCOPY_EXAM_FINDING is null)
                                  THEN 'HPV_Positive-Requires VIA Triage'
                              WHEN FollowUP.screening_type = 'Initial visit' AND
                                   (FollowUP.CCS_VIA_Result = 'VIA positive: eligible for cryo/thermo-coagula' OR
                                    FollowUP.CCS_VIA_Result = 'VIA positive: non-eligible for cryo/thermo-coagula' OR
                                    (((FollowUP.CCS_VIA_Result is null OR FollowUP.CCS_VIA_Result = 'Unknown') AND
                                      (FollowUP.CYTOLOGY_RESULT = '> Ascus') AND
                                      (FollowUP.COLPOSCOPY_EXAM_FINDING = 'Low Grade' OR
                                       FollowUP.COLPOSCOPY_EXAM_FINDING = 'High Grade'))))
                                  THEN 'HPV_Positive'
                              WHEN FollowUP.screening_type = 'Re-screening for cervical cancer after 1 year' AND
                                   (FollowUP.CCS_VIA_Result = 'VIA positive: eligible for cryo/thermo-coagula' OR
                                    FollowUP.CCS_VIA_Result = 'VIA positive: non-eligible for cryo/thermo-coagula')
                                  THEN 'VIA_Positive'
                              WHEN FollowUP.screening_type = 'Post-treatment follow-up at 1 year' AND
                                   (FollowUP.Cytology_Result = '> Ascus' AND
                                    (FollowUP.COLPOSCOPY_EXAM_FINDING is null OR
                                     FollowUP.COLPOSCOPY_EXAM_FINDING = 'Low Grade' OR
                                     FollowUP.COLPOSCOPY_EXAM_FINDING = 'High Grade'))
                                  THEN 'CYT_Positive'
                              WHEN FollowUP.screening_type = 'Initial visit' AND (FollowUP.CCS_HPV_Result = 'Positive'
                                  AND ((FollowUP.CCS_VIA_Result = 'VIA negative') OR
                                       ((FollowUP.CCS_VIA_Result is null OR FollowUP.CCS_VIA_Result = 'Unknown') AND
                                        (FollowUP.COLPOSCOPY_EXAM_FINDING = 'Normal' OR
                                         (FollowUP.CYTOLOGY_RESULT = 'Negative result' OR FollowUP.CYTOLOGY_RESULT =
                                                                                          'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear')))))
                                  THEN 'HPV_Negative'
                              WHEN FollowUP.screening_type = 'Initial visit' AND
                                   (FollowUP.CCS_HPV_Result = 'Negative result')
                                  THEN 'HPV_Negative'
                              WHEN FollowUP.screening_type = 'Re-screening for cervical cancer after 1 year' AND
                                   (FollowUP.CCS_VIA_Result = 'VIA negative')
                                  THEN 'VIA_Negative'
                              WHEN FollowUP.screening_type = 'Post-treatment follow-up at 1 year' AND
                                   (FollowUP.Cytology_Result = 'Negative result' OR FollowUP.Cytology_Result =
                                                                                    'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear') OR
                                   (FollowUP.Cytology_Result = '> Ascus' AND
                                    FollowUP.COLPOSCOPY_EXAM_FINDING = 'Normal')
                                  THEN 'CYT_Negative'
                              WHEN FollowUP.CCS_VIA_Result = 'VIA positive: non-eligible for cryo/thermo-coagula'
                                  THEN 'VIA_Suspected' END             AS Prev_CCS_Screen_Result,
                          CASE
                              when (FollowUP.CCS_Precancerous_Treat = 'Cryosurgery of lesion of cervix') and
                                   ((FollowUP.CCS_Treat_Received_Date < REPORT_START_DATE And
                                     FollowUP.CCS_Treat_Received_Date is not null) OR
                                    FollowUP.follow_up_date < REPORT_START_DATE) THEN 'Cryotherapy'
                              when (FollowUP.CCS_Precancerous_Treat =
                                    'Loop electrosurgical excision procedure of cervix') and
                                   ((FollowUP.CCS_Treat_Received_Date < REPORT_START_DATE
                                       And FollowUP.CCS_Treat_Received_Date is not null) OR
                                    FollowUP.follow_up_date < REPORT_START_DATE)
                                  THEN 'LEEP'
                              when (FollowUP.CCS_Precancerous_Treat = 'Thermocauterization of cervix') and
                                   ((FollowUP.CCS_Treat_Received_Date < REPORT_START_DATE And
                                     FollowUP.CCS_Treat_Received_Date is not null) OR
                                    FollowUP.follow_up_date < REPORT_START_DATE)
                                  THEN 'Thermocoagulation' end         as Prev_CxCa_TreatmentGiven,
                          CASE
                              when FollowUP.CCS_Treat_Received_Date is not null
                                  then CCS_Treat_Received_Date
                              else FollowUP.follow_up_date end         as Prev_CxCaTreatmentGivenDate

                   from tmp_prev_cxca_1
                            inner join FollowUP on FollowUP.encounter_id = tmp_prev_cxca_1.encounter_id),

     tmp_curr_cxca as (select encounter_id,
                              client_id,
                              follow_up_date                                                                             As SelectedFollowUpDate,
                              ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                       from FollowUP
                       where follow_up_date >= REPORT_START_DATE
                         and screening_status = 'Cervical cancer screening performed'),


     tmp_curr_cxca_1 as (select * from tmp_curr_cxca where row_num = 1),

     curr_cxca as (select CASE
                              WHEN FollowUP.colposcopy_exam_date IS NOT NULL and
                                   FollowUP.colposcopy_exam_date is not null
                                  THEN FollowUP.colposcopy_exam_date
                              WHEN FollowUP.date_cytology_result_received IS NOT NULL and
                                   FollowUP.date_cytology_result_received is not null
                                  THEN FollowUP.date_cytology_result_received
                              WHEN FollowUP.date_via_result IS NOT NULL and FollowUP.date_via_result is not null
                                  THEN FollowUP.date_via_result
                              WHEN (FollowUP.HPV_DNA_RESULT_RECEIVED_DATE IS NOT NULL
                                  and FollowUP.HPV_DNA_RESULT_RECEIVED_DATE is not null) AND
                                   FollowUP.CCS_HPV_Result = 'Negative result'
                                  THEN FollowUP.HPV_DNA_RESULT_RECEIVED_DATE
                              WHEN FollowUP.follow_up_date is not null and
                                   FollowUP.screening_status = 'Cervical cancer screening performed'
                                  THEN FollowUP.follow_up_date END     AS Curr_CSS_Screen_Done_Date_Calculated,
                          FollowUP.screening_type                      as Curr_Screen_Type,
                          FollowUP.screening_method                    as Curr_Screen_Method,
                          FollowUP.HPV_SUBTYPE                         as Curr_HPV_SubType,
                          FollowUP.date_hpv_test_was_done              As Curr_HPV_DAN_SampleCollected_Date,
                          FollowUP.HPV_DNA_RESULT_RECEIVED_DATE        As Curr_HPV_DAN_ResultReceived_Date,
                          FollowUP.CCS_HPV_Result                      As Curr_HPV_Result,
                          FollowUP.date_via_result                     As Curr_VIA_Screening_Date,
                          FollowUP.CCS_VIA_Result                      As Curr_VIA_Screening_Result,
                          FollowUP.CYTOLOGY_SAMPLE_COLLECTION_DATE     As Curr_Cytology_SampleCollected_Date,
                          FollowUP.date_cytology_result_received       As Curr_Cytology_ResultReceived_Date,
                          FollowUP.ccs_next_date,
                          FollowUP.CYTOLOGY_RESULT                     As Curr_Cytology_Result,
                          FollowUP.colposcopy_exam_date                As Curr_Colposcopy_Exam_Date,
                          FollowUP.COLPOSCOPY_EXAM_FINDING             As Curr_Colposcopy_Exam_Result,
                          FollowUP.BIOPSY_SAMPLE_COLLECTED_DATE        As Curr_Biopsy_SampleCollected_Date,
                          FollowUP.BIOPSY_RESULT_RECEIVED_DATE         As Curr_Biopsy_ResultReceived_Date,
                          FollowUP.BIOPSY_RESULT                       As Curr_Biopsy_Result,
                          FollowUP.CCS_Precancerous_Treat              As Curr_TX_Received_for_PrecancerousLesion,
                          FollowUP.confirmed_cervical_cancer_cases_bas As Curr_TX_for_ConfirmedCxCaBasedOn_Biopsy,
                          FollowUP.CCS_Treat_Received_Date             As Curr_Date_TX_Given,
                          FollowUP.REFERRAL_OR_LINKAGE_STATUS          As Curr_ReferralStatus,
                          FollowUP.reason_for_referral_cacx            As Curr_Reason_for_Referral,
                          FollowUP.date_patient_referred_out           As Curr_Date_Referred_to_OtherHF,
                          FollowUP.date_client_arrived_in_the_referred As Curr_Date_Client_Arrived_in_RefferedHF,
                          FollowUP.date_client_served_in_the_referred_ AS Curr_Date_Client_Served_in_RefferedHF,
                          CCaCounsellingGiven
                                                                       as Counselled,
                          Accepted                                     as Accepted,
                          CASE
                              WHEN FollowUP.screening_method = 'Human Papillomavirus test' AND
                                   (FollowUP.CCS_VIA_Result is null) AND
                                   (FollowUP.CYTOLOGY_RESULT is null) AND
                                   (FollowUP.COLPOSCOPY_EXAM_FINDING is null) And
                                   (FollowUP.COLPOSCOPY_EXAM_FINDING is null)
                                  THEN 'HPV_Positive-Requires VIA Triage'
                              WHEN FollowUP.screening_method = 'Human Papillomavirus test' AND
                                   (FollowUP.CCS_VIA_Result = 'VIA positive: eligible for cryo/thermo-coagula' OR
                                    FollowUP.CCS_VIA_Result = 'VIA positive: non-eligible for cryo/thermo-coagula' OR
                                    (((FollowUP.CCS_VIA_Result is null OR FollowUP.CCS_VIA_Result = 'Unknown') AND
                                      (FollowUP.CYTOLOGY_RESULT = '> Ascus')
                                        AND
                                      (FollowUP.COLPOSCOPY_EXAM_FINDING = 'Low Grade' OR
                                       FollowUP.COLPOSCOPY_EXAM_FINDING = 'High Grade'))))
                                  THEN 'HPV_Positive'
                              WHEN FollowUP.screening_method =
                                   'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND
                                   (FollowUP.CCS_VIA_Result = 'VIA positive: eligible for cryo/thermo-coagula' OR
                                    FollowUP.CCS_VIA_Result = 'VIA positive: non-eligible for cryo/thermo-coagula')
                                  THEN 'VIA_Positive'
                              WHEN FollowUP.screening_method = 'Cytology' AND (FollowUP.Cytology_Result = '> Ascus' AND
                                                                               (FollowUP.COLPOSCOPY_EXAM_FINDING is null OR
                                                                                FollowUP.COLPOSCOPY_EXAM_FINDING =
                                                                                'Low Grade' OR
                                                                                FollowUP.COLPOSCOPY_EXAM_FINDING = '2'))
                                  THEN 'CYT_Positive'
                              WHEN FollowUP.screening_method = 'Human Papillomavirus test' AND
                                   (FollowUP.CCS_HPV_Result = 'Positive' AND
                                    ((FollowUP.CCS_VIA_Result = 'VIA negative') OR
                                     ((FollowUP.CCS_VIA_Result is null OR FollowUP.CCS_VIA_Result = 'Unknown') AND
                                      (FollowUP.COLPOSCOPY_EXAM_FINDING =
                                       'Normal' OR
                                       (FollowUP.CYTOLOGY_RESULT =
                                        'Negative result' OR
                                        FollowUP.CYTOLOGY_RESULT =
                                        'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear')))))
                                  THEN 'HPV_Negative'
                              WHEN FollowUP.screening_method = 'Human Papillomavirus test' AND
                                   (FollowUP.CCS_HPV_Result = 'Negative result')
                                  THEN 'HPV_Negative'
                              WHEN FollowUP.screening_method =
                                   'Visual Inspection of the Cervix with Acetic Acid (VIA)' AND
                                   (FollowUP.CCS_VIA_Result = 'VIA negative')
                                  THEN 'VIA_Negative'
                              WHEN FollowUP.screening_method = 'Cytology' AND
                                   (FollowUP.Cytology_Result = 'Negative result' OR FollowUP.Cytology_Result =
                                                                                    'ASCUS (Atypical Squamous Cells of Undetermined Significance) on Pap Smear') OR
                                   (FollowUP.Cytology_Result = '> Ascus' AND
                                    FollowUP.COLPOSCOPY_EXAM_FINDING = 'Normal')
                                  THEN 'CYT_Negative'
                              WHEN FollowUP.CCS_VIA_Result = 'VIA positive: non-eligible for cryo/thermo-coagula'
                                  THEN 'VIA_Suspected' END             AS Curr_CCS_Screen_Result,
                          CASE
                              when (FollowUP.CCS_Precancerous_Treat = 'Cryosurgery of lesion of cervix') and
                                   ((FollowUP.CCS_Treat_Received_Date between REPORT_START_DATE and REPORT_END_DATE And
                                     FollowUP.CCS_Treat_Received_Date is not null) OR
                                    FollowUP.follow_up_date between REPORT_START_DATE and REPORT_END_DATE) THEN 'Cryotherapy'
                              when (FollowUP.CCS_Precancerous_Treat =
                                    'Loop electrosurgical excision procedure of cervix') and
                                   ((FollowUP.CCS_Treat_Received_Date between REPORT_START_DATE and REPORT_END_DATE And
                                     FollowUP.CCS_Treat_Received_Date is not null) OR
                                    FollowUP.follow_up_date between REPORT_START_DATE and REPORT_END_DATE) THEN 'LEEP'
                              when (FollowUP.CCS_Precancerous_Treat = 'Thermocauterization of cervix') and
                                   ((FollowUP.CCS_Treat_Received_Date between REPORT_START_DATE and REPORT_END_DATE And
                                     FollowUP.CCS_Treat_Received_Date is not null) OR
                                    FollowUP.follow_up_date between REPORT_START_DATE and REPORT_END_DATE)
                                  THEN 'Thermocoagulation'
                              end                                      as Curr_CxCa_TreatmentGiven,
                          CASE
                              when FollowUP.CCS_Treat_Received_Date is not null
                                  then CCS_Treat_Received_Date
                              else FollowUP.follow_up_date end         as Curr_CxCaTreatmentGivenDate
                   from FollowUP
                            join tmp_curr_cxca_1 on FollowUP.encounter_id = tmp_curr_cxca_1.encounter_id),

     tmp_tx_curr as (select encounter_id,
                            client_id,
                            follow_up_status,
                            art_end_date,
                            art_start_date,
                            ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                     from FollowUP
                     where follow_up_date <= REPORT_END_DATE),
     tx_curr as (select *
                 from tmp_tx_curr
                 where row_num = 1
                   and follow_up_status in ('Alive', 'Restart medication')
                   and art_end_date >= REPORT_END_DATE and  FLOOR(DATEDIFF(REPORT_END_DATE, tmp_tx_curr.art_start_date) / 30.4375) >=0),
     cxca_final as (select client.sex                                       as Sex,
                           FollowUP.weight_text_                                 as Weight,
                           timestampdiff(YEAR, client.date_of_birth, REPORT_END_DATE) as Age,
                           FollowUP.follow_up_date,
                           FollowUP.art_start_date                               AS ArtStartDate,
                           tx_curr.follow_up_status                              AS FollowUpStatus,
                           FollowUP.next_visit_date,
                           FollowUP.regimen                                      as ARVRegimen,
                           FollowUP.regimen                                         RegimenLine,
                           FollowUP.dose_days                                    As ARTDoseDays,
                           prev_cxca.Prev_CSS_Screen_Done_Date_Calculated,
                           prev_cxca.CCS_Next_Date                               AS Prev_AppointmentDate_4_CCS,
                           CASE
                               WHEN prev_cxca.PrevCxCaStatus IS NULL AND prev_cxca.screening_status IS NULL
                                   THEN 'Never Screened'
                               WHEN prev_cxca.PrevCxCaStatus IS NULL AND (prev_cxca.CCS_Next_Date is null
                                   Or prev_cxca.CCS_Next_Date > REPORT_END_DATE) THEN 'Not Eligible'
                               ELSE prev_cxca.PrevCxCaStatus END                 AS EligibilityReason,
                           prev_cxca.Prev_Screen_Type,
                           prev_cxca.Prev_Screen_Method,
                           prev_cxca.Prev_HPV_SubType,
                           prev_cxca.Prev_HPV_DAN_SampleCollected_Date,
                           prev_cxca.Prev_HPV_DAN_ResultReceived_Date,
                           prev_cxca.Prev_HPV_Result,
                           prev_cxca.Prev_VIA_Screening_Date,
                           prev_cxca.Prev_VIA_Screening_Result,
                           prev_cxca.Prev_Cytology_SampleCollected_Date,
                           prev_cxca.Prev_Cytology_ResultReceived_Date,
                           prev_cxca.Prev_Cytology_Result,
                           prev_cxca.Prev_Colposcopy_Exam_Date,
                           prev_cxca.Prev_Colposcopy_Exam_Result,
                           prev_cxca.Prev_Biopsy_SampleCollected_Date,
                           prev_cxca.Prev_Biopsy_ResultReceived_Date,
                           prev_cxca.Prev_Biopsy_Result,
                           prev_cxca.Prev_TX_Received_for_PrecancerousLesion,
                           prev_cxca.Prev_TX_for_ConfirmedCxCaBasedOn_Biopsy,
                           prev_cxca.Prev_Date_TX_Given,
                           prev_cxca.Prev_ReferralStatus,
                           prev_cxca.Prev_Reason_for_Referral,
                           prev_cxca.Prev_Date_Referred_to_OtherHF,
                           prev_cxca.Prev_Date_Client_Arrived_in_RefferedHF,
                           prev_cxca.Prev_Date_Client_Served_in_RefferedHF,
                           prev_cxca.Prev_CCS_Screen_Result,
                           CASE
                               WHEN FollowUP.follow_up_date between REPORT_START_DATE and REPORT_END_DATE THEN 'Yes'
                               ELSE 'No' END                                     AS Seen,
                           curr_cxca.Curr_CSS_Screen_Done_Date_Calculated,
                           curr_cxca.Counselled,
                           curr_cxca.Accepted,
                           curr_cxca.Curr_Screen_Type,
                           curr_cxca.Curr_Screen_Method,
                           curr_cxca.Curr_HPV_SubType,
                           curr_cxca.Curr_HPV_DAN_SampleCollected_Date,
                           curr_cxca.Curr_HPV_DAN_ResultReceived_Date,
                           curr_cxca.Curr_HPV_Result,
                           curr_cxca.Curr_VIA_Screening_Date,
                           curr_cxca.Curr_VIA_Screening_Result,
                           curr_cxca.Curr_Cytology_SampleCollected_Date,
                           curr_cxca.Curr_Cytology_ResultReceived_Date,
                           curr_cxca.Curr_Cytology_Result,
                           curr_cxca.Curr_Colposcopy_Exam_Date,
                           curr_cxca.Curr_Colposcopy_Exam_Result,
                           curr_cxca.Curr_Biopsy_SampleCollected_Date,
                           curr_cxca.Curr_Biopsy_ResultReceived_Date,
                           curr_cxca.Curr_Biopsy_Result,
                           curr_cxca.Curr_TX_Received_for_PrecancerousLesion,
                           curr_cxca.Curr_TX_for_ConfirmedCxCaBasedOn_Biopsy,
                           curr_cxca.Curr_Date_TX_Given,
                           curr_cxca.Curr_ReferralStatus,
                           curr_cxca.Curr_Reason_for_Referral,
                           curr_cxca.Curr_Date_Referred_to_OtherHF,
                           curr_cxca.Curr_Date_Client_Arrived_in_RefferedHF,
                           curr_cxca.Curr_Date_Client_Served_in_RefferedHF,
                           curr_cxca.Curr_CCS_Screen_Result,
                           curr_cxca.CCS_Next_Date                               AS Next_AppointmentDate_4_CCS,
                           client.patient_uuid                                         as PatientGUID,
                           client.mrn                                          as MRN,
                           tx_curr.client_id

                    from tx_curr
                             join FollowUP on tx_curr.encounter_id = FollowUP.encounter_id
                             left join mamba_dim_client client on tx_curr.client_id=client.client_id
                             Left join prev_cxca on tx_curr.client_id = prev_cxca.client_id
                             left join curr_cxca on tx_curr.client_id = prev_cxca.client_id

                    where timestampdiff(YEAR, client.date_of_birth, REPORT_END_DATE) BETWEEN 15
                        AND 100
                      AND client.sex = 'FEMALE'
                      AND FollowUP.follow_up_date
                        < REPORT_END_DATE
                      AND FollowUP.art_start_date is not null)
select CASE Sex
        WHEN 'FEMALE' THEN 'F'
        WHEN 'MALE'   THEN 'M'
        end as Sex,
       Weight,
       Age,
       cxca_final.follow_up_date as FollowUpDate,
       ArtStartDate,
       FollowUpStatus,
       next_visit_date,
       ARVRegimen,
       LEFT(RegimenLine,1) as RegimenLine,
       ARTDoseDays,
       Prev_CSS_Screen_Done_Date_Calculated,
       Prev_AppointmentDate_4_CCS,
       EligibilityReason,
       Prev_Screen_Type,
       Prev_Screen_Method,
       Prev_HPV_SubType,
       Prev_HPV_DAN_SampleCollected_Date,
       Prev_HPV_DAN_ResultReceived_Date,
       Prev_HPV_Result,
       Prev_VIA_Screening_Date,
       Prev_VIA_Screening_Result,
       Prev_Cytology_SampleCollected_Date,
       Prev_Cytology_ResultReceived_Date,
       Prev_Cytology_Result,
       Prev_Colposcopy_Exam_Date,
       Prev_Colposcopy_Exam_Result,
       Prev_Biopsy_SampleCollected_Date,
       Prev_Biopsy_ResultReceived_Date,
       Prev_Biopsy_Result,
       Prev_TX_Received_for_PrecancerousLesion,
       Prev_TX_for_ConfirmedCxCaBasedOn_Biopsy,
       Prev_Date_TX_Given,
       Prev_ReferralStatus,
       Prev_Reason_for_Referral,
       Prev_Date_Referred_to_OtherHF,
       Prev_Date_Client_Arrived_in_RefferedHF,
       Prev_Date_Client_Served_in_RefferedHF,
       Prev_CCS_Screen_Result,
       Seen,
       Curr_CSS_Screen_Done_Date_Calculated,
       Counselled,
       Accepted,
       Curr_Screen_Type,
       Curr_Screen_Method,
       Curr_HPV_SubType,
       Curr_HPV_DAN_SampleCollected_Date,
       Curr_HPV_DAN_ResultReceived_Date,
       Curr_HPV_Result,
       Curr_VIA_Screening_Date,
       Curr_VIA_Screening_Result,
       Curr_Cytology_SampleCollected_Date,
       Curr_Cytology_ResultReceived_Date,
       Curr_Cytology_Result,
       Curr_Colposcopy_Exam_Date,
       Curr_Colposcopy_Exam_Result,
       Curr_Biopsy_SampleCollected_Date,
       Curr_Biopsy_ResultReceived_Date,
       Curr_Biopsy_Result,
       Curr_TX_Received_for_PrecancerousLesion,
       Curr_TX_for_ConfirmedCxCaBasedOn_Biopsy,
       Curr_Date_TX_Given,
       Curr_ReferralStatus,
       Curr_Reason_for_Referral,
       Curr_Date_Referred_to_OtherHF,
       Curr_Date_Client_Arrived_in_RefferedHF,
       Curr_Date_Client_Served_in_RefferedHF,
       Curr_CCS_Screen_Result,
       Next_AppointmentDate_4_CCS,
       PatientGUID
from cxca_final
where EligibilityReason != 'Not Eligible';