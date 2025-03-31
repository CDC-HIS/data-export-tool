WITH FollowUp AS (select follow_up.encounter_id,
                         follow_up.client_id                       AS PatientId,
                         follow_up_status,
                         follow_up_date_followup_                  AS follow_up_date,
                         follow_up_2.art_antiretroviral_start_date AS art_start_date,
                         assessment_date,
                         treatment_end_date,
                         antiretroviral_art_dispensed_dose_i       AS ARTDoseDays,
                         follow_up.weight_text_                              AS Weight,
                         screening_test_result_tuberculosis        AS TB_SreeningStatus,
                         follow_up_3.date_of_last_menstrual_period_lmp_           LMP_Date,
                         anitiretroviral_adherence_level           AS AdherenceLevel,
                         next_visit_date,
                         follow_up.regimen,
                         currently_breastfeeding_child                breast_feeding_status,
                         follow_up_1.pregnancy_status,
                         diagnosis_date                            AS ActiveTBDiagnoseddate,
                         nutritional_status_of_adult,
                         nutritional_supplements_provided,
                         stages_of_disclosure,
                         date_started_on_tuberculosis_prophy,
                         method_of_family_planning,
                         patient_diagnosed_with_active_tuber       as ActiveTBDiagnosed,
                         dsd_category,
                         nutritional_screening_result,
                         inh_start_date,
                         inh_date_completed,
                         eats_nutritious_foods
                  FROM mamba_flat_encounter_follow_up follow_up
                           JOIN mamba_flat_encounter_follow_up_1 follow_up_1
                                ON follow_up.encounter_id = follow_up_1.encounter_id
                           JOIN mamba_flat_encounter_follow_up_2 follow_up_2
                                ON follow_up.encounter_id = follow_up_2.encounter_id
                           LEFT JOIN mamba_flat_encounter_follow_up_3 follow_up_3
                                     ON follow_up.encounter_id = follow_up_3.encounter_id
                           LEFT join mamba_flat_encounter_intake_b intake_b
                                     on follow_up.client_id = intake_b.client_id),
     -- TX curr
     tx_curr_all AS (SELECT PatientId,
                            follow_up_date                                                                             AS FollowupDate,
                            encounter_id,
                            treatment_end_date,
                            follow_up_status,
                            art_start_date,
                            ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY follow_up_date DESC, encounter_id DESC) AS row_num
                     FROM FollowUp
                     WHERE follow_up_status IS NOT NULL
                       AND art_start_date IS NOT NULL
                       AND follow_up_date <= REPORT_END_DATE),
     latestDSD_tmp AS (SELECT PatientId,
                              follow_up_date                                                                               AS latestDsdDate,
                              encounter_id,
                              dsd_category,
                              ROW_NUMBER() OVER (PARTITION BY PatientId ORDER BY follow_up_date DESC , encounter_id DESC ) AS row_num
                       FROM FollowUp
                       WHERE assessment_date IS NOT NULL
                         AND follow_up_date <= REPORT_END_DATE
                         AND assessment_date <= REPORT_END_DATE),

     latestDSD AS (select * from latestDSD_tmp where row_num = 1),
     tx_curr AS (select *
                 from tx_curr_all
                 where row_num = 1
                   and treatment_end_date >= REPORT_END_DATE
                   AND follow_up_status in ('Alive', 'Restart medication')
                   and TIMESTAMPDIFF(DAY, art_start_date, REPORT_END_DATE) >= 0)


select CASE client.Sex
           WHEN 'FEMALE' THEN 'F'
           WHEN 'MALE' THEN 'M'
           end                                                                                        as Sex,
       Weight,
       TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE)                                            as Age,
       fn_gregorian_to_ethiopian_calendar(follow_up_date, 'D/M/Y')                                    as FollowUpDate,
       follow_up_date                                                                                 as FollowUpDate_GC,
       fn_gregorian_to_ethiopian_calendar(next_visit_date, 'D/M/Y')                                   as Next_visit_Date,
       next_visit_date                                                                                as Next_visit_Date_GC,
       left(regimen, 2)                                                                               as ARVRegimen,
       left(regimen, 1)                                                                               as RegimensLine,
       ARTDoseDays,
       tx_curr.follow_up_status                                                                       as FollowupStatus,
       fn_gregorian_to_ethiopian_calendar(FollowUp.treatment_end_date, 'D/M/Y')                       as ARTDoseEndDate,
       FollowUp.treatment_end_date                                                                    as ARTDoseEndDate_DC,
       AdherenceLevel                                                                                 as AdheranceLevel,
       fn_gregorian_to_ethiopian_calendar(tx_curr.art_start_date, 'D/M/Y')                            as ARTStartDate,
       tx_curr.art_start_date                                                                         as ARTStartDate_GC,
       fn_gregorian_to_ethiopian_calendar(inh_start_date, 'D/M/Y')                                    as INH_Start_Date,
       inh_start_date                                                                                 as INH_Start_Date_GC,
       fn_gregorian_to_ethiopian_calendar(inh_date_completed, 'D/M/Y')                                as INH_Completed_Date,
       inh_date_completed                                                                             as INH_Completed_Date_GC,
       CASE
           WHEN method_of_family_planning = 'Intrauterine device' OR
                method_of_family_planning = 'Vasectomy' OR
                method_of_family_planning = 'Tubal ligation procedure' OR
                method_of_family_planning = 'Implantable contraceptive (unspecified type)' OR
                method_of_family_planning = 'None' THEN 'LongTermFP'
           WHEN method_of_family_planning = 'Diaphragm' OR
                method_of_family_planning = 'Oral contraception' OR
                method_of_family_planning = 'Injectable contraceptives' OR
                method_of_family_planning = 'Condoms' THEN 'ShortTermFP' END                          AS FP_Status,
       CASE TB_SreeningStatus
           WHEN 'Positive' THEN 'TB_Positive'
           ELSE TB_SreeningStatus END                                                                 as TB_SreeningStatus
        ,
       ActiveTBDiagnosed,
       nutritional_screening_result                                                                   as NutritionalScrenningStatus,
       CASE
           When nutritional_status_of_adult is not null then
               Case
                   when TIMESTAMPDIFF(YEAR, date_of_birth, REPORT_END_DATE) BETWEEN 15 AND 49 Then
                       Case
                           When 'FEMALE' Then
                               Case
                                   when pregnancy_status = 'No'
                                       then 'Female:NotPregnant'
                                   when pregnancy_status = 'Yes'
                                       then 'Female:Pregnant'
                                   else 'Female:NotPregnant'
                                   End
                           else client.Sex end
                   else client.Sex end
           end
                                                                                                      As SexForNutrition,
       eats_nutritious_foods                                                                          as TherapeuticFoodProvided,
       patient_uuid                                                                                   as PatientGUID,
       pregnancy_status                                                                               as IsPregnant,
       breast_feeding_status                                                                          as BreastFeeding,
       fn_gregorian_to_ethiopian_calendar(LMP_Date, 'D/M/Y')                                          as LMP_Date,
       LMP_Date                                                                                       as LMP_Date_GC,
       PERIOD_DIFF(date_format(REPORT_END_DATE, '%Y%m'), date_format(tx_curr.art_start_date, '%Y%m')) as MonthsOnART,
       FollowUp.DSD_Category,
       stages_of_disclosure                                                                           as ChildDisclosueStatus
from FollowUp
         inner join tx_curr on FollowUp.encounter_id = tx_curr.encounter_id
    --  left join latestDSD on latestDSD.PatientId = tx_curr.PatientId
         left join mamba_dim_client client on tx_curr.PatientId = client.client_id
;