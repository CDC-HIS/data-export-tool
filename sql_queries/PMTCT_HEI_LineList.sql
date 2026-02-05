WITH HEI_Enrollment AS (
    SELECT
        client_id,
        encounter_id,
        date_enrolled_in_care,
        hei_code,
        infant_referred,
        referring_facility_name,
        arv_prophylaxis,
        weight_text,
        mothers_pmtct_interventions,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY date_enrolled_in_care DESC, encounter_id DESC) as row_num
    FROM mamba_flat_encounter_hei_enrollment
    WHERE date_enrolled_in_care <= REPORT_END_DATE
),
HEI_FollowUp AS (
    SELECT
        f.encounter_id,
        f.client_id,
        f.followup_date_followup AS follow_up_date,
        f.weight_text_ AS Weight,
        f.growth_pattern,
        f.reason_for_growth_failure,
        f.reason_for_red_flag,
        f.mother_s_breast_condition,
        f.next_visit_date,
        f.conclusion,
        f.decision,
        f.continue_to_followup,
        f.transferred_out,
        f.lost_to_followup,
        f.died,
        f1.cotrimoxazole_prophylaxis_dose,
        f1.developmental_milestone_for_children,
        COALESCE(f1.infant_feeding_practice_within_the_first_6_months_of_life, f1.infant_feeding_practice_older_than_6_months_of_life) AS infant_feeding_practice,
        f1.no_clinical_or_laboratory_evidence_of_hiv,
        f.clinical_evidence_of_hiv,
        f1.laboratory_evidence_of_hiv_dna_pcr_antibody_tests,
        f1.referred_for_pediatric_hiv_care_within_facility,
        f1.referred_pediatric_hiv_care_outside_facility,
        f1.discharged_negative_form_care_hiv_free,
        ROW_NUMBER() OVER (PARTITION BY f.client_id ORDER BY f.followup_date_followup DESC, f.encounter_id DESC) as row_num
    FROM mamba_flat_encounter_hei_followup f
    LEFT JOIN mamba_flat_encounter_hei_followup_1 f1 ON f.encounter_id = f1.encounter_id
    WHERE f.followup_date_followup BETWEEN REPORT_START_DATE AND REPORT_END_DATE
),
HEI_Lab AS (
    SELECT
        l.client_id,
        l.encounter_id,
        l.dna_pcr_sample_collection_date,
        l.date_dbs_result_received,
        l.hiv_test_result,
        l.rapid_antibody_result,
        l.test_type,
        l.reason_sample_rejected_or_test_not_done,
        ROW_NUMBER() OVER (PARTITION BY l.client_id ORDER BY l.dna_pcr_sample_collection_date DESC, l.encounter_id DESC) as row_num
    FROM mamba_flat_encounter_hei_hiv_test l
    WHERE l.dna_pcr_sample_collection_date <= REPORT_END_DATE
),
Immunization AS (
    SELECT
        client_id,
        CONCAT_WS(', ',
            CASE WHEN MAX(bcg_taken) = 'Yes' THEN 'BCG' END,
            CASE WHEN MAX(opv_0_taken) = 'Yes' THEN 'OPV0' END,
            CASE WHEN MAX(opv_1_taken) = 'Yes' THEN 'OPV1' END,
            CASE WHEN MAX(opv_2_taken) = 'Yes' THEN 'OPV2' END,
            CASE WHEN MAX(opv_3_taken) = 'Yes' THEN 'OPV3' END,
            CASE WHEN MAX(ipv_taken) = 'Yes' THEN 'IPV' END,
            CASE WHEN MAX(pcv_0_taken) = 'Yes' THEN 'PCV0' END,
            CASE WHEN MAX(pcv_1_taken) = 'Yes' THEN 'PCV1' END,
            CASE WHEN MAX(pcv_2_taken) = 'Yes' THEN 'PCV2' END,
            CASE WHEN MAX(pcv_3_taken) = 'Yes' THEN 'PCV3' END,
            CASE WHEN MAX(rota_0_taken) = 'Yes' THEN 'ROTA0' END,
            CASE WHEN MAX(rota_1_taken) = 'Yes' THEN 'ROTA1' END,
            CASE WHEN MAX(rota_2_taken) = 'Yes' THEN 'ROTA2' END,
            CASE WHEN MAX(penta_1_taken) = 'Yes' THEN 'PENTA1' END,
            CASE WHEN MAX(penta_2_taken) = 'Yes' THEN 'PENTA2' END,
            CASE WHEN MAX(penta_3_taken) = 'Yes' THEN 'PENTA3' END,
            CASE WHEN MAX(mcv_1_taken) = 'Yes' THEN 'MCV1' END,
            CASE WHEN MAX(mcv_2_taken) = 'Yes' THEN 'MCV2' END
        ) as Immunizations
    FROM mamba_flat_encounter_hei_immunization
    GROUP BY client_id
),
CPT_Start AS (
    SELECT
        client_id,
        CPTStartDate,
        CPTDose
    FROM (
        SELECT
            f.client_id,
            f.followup_date_followup AS CPTStartDate,
            f1.cotrimoxazole_prophylaxis_dose AS CPTDose,
            ROW_NUMBER() OVER (PARTITION BY f.client_id ORDER BY f.followup_date_followup ASC) as rn
        FROM mamba_flat_encounter_hei_followup f
        JOIN mamba_flat_encounter_hei_followup_1 f1 ON f.encounter_id = f1.encounter_id
        WHERE f1.cotrimoxazole_prophylaxis_dose IS NOT NULL AND f1.cotrimoxazole_prophylaxis_dose != ''
    ) t
    WHERE rn = 1
),
CPT_Stop AS (
    SELECT
        f.client_id,
        MAX(f.followup_date_followup) AS CPTDiscontinuedDate
    FROM mamba_flat_encounter_hei_followup f
    JOIN mamba_flat_encounter_hei_followup_1 f1 ON f.encounter_id = f1.encounter_id
    WHERE f1.cotrimoxazole_prophylaxis_dose = 'DC'
    GROUP BY f.client_id
),
FinalOutcome AS (
    SELECT
        client_id,
        hei_pmtct_final_outcome,
        date_when_final_outcome_was_known,
        name_of_where_patient_was_referred_to,
        ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY date_when_final_outcome_was_known DESC) as row_num
    FROM mamba_flat_encounter_hei_final_outcome
    WHERE date_when_final_outcome_was_known <= REPORT_END_DATE
),
Future_Visit AS (
    SELECT client_id
    FROM mamba_flat_encounter_hei_followup
    WHERE followup_date_followup > REPORT_END_DATE
    GROUP BY client_id
)

SELECT
    client.patient_uuid AS PatientGUID,
    CASE client.sex WHEN 'FEMALE' THEN 'F' WHEN 'MALE' THEN 'M' END AS Sex,
    client.date_of_birth AS DOB,
    TIMESTAMPDIFF(YEAR, client.date_of_birth, REPORT_END_DATE) AS AgeYear,
    TIMESTAMPDIFF(MONTH, client.date_of_birth, f.follow_up_date) AS AgeMonth,
    TIMESTAMPDIFF(WEEK, client.date_of_birth, f.follow_up_date) AS AgeInWeeks,
    enr.weight_text AS BirthWeight,
    enr.hei_code AS HEICode,
    enr.infant_referred AS InfantReferred,
    enr.referring_facility_name AS InfantReferingFacility,
    enr.date_enrolled_in_care AS EnrollmentDate,
    fn_gregorian_to_ethiopian_calendar(enr.date_enrolled_in_care, 'D/M/Y') AS EnrollmentDateET,
    enr.arv_prophylaxis AS InfantARVProphylaxis,
    enr.mothers_pmtct_interventions AS MotherPMTCTIntervention,
    f.follow_up_date AS FollowupDate,
    fn_gregorian_to_ethiopian_calendar(f.follow_up_date, 'D/M/Y') AS FollowupDateET,
    f.Weight,
    f.growth_pattern AS GrowthPattern,
    f.reason_for_growth_failure AS ReasonforGrowthFailure,
    f.developmental_milestone_for_children AS DevelopmentMilestone,
    f.reason_for_red_flag AS ReasonforRedFlag,
    f.infant_feeding_practice AS InfantFeedingPractice,
    f.mother_s_breast_condition AS MotherBreastCondition,
    lab.rapid_antibody_result AS RapidAntibodyTest,
    lab.test_type AS TestIndication,
    lab.dna_pcr_sample_collection_date AS DNAPCRSampleCollectionDate,
    fn_gregorian_to_ethiopian_calendar(lab.dna_pcr_sample_collection_date, 'D/M/Y') AS DNAPCRSampleCollectionDateET,
    lab.date_dbs_result_received AS DateofDBSResultReceived,
    lab.hiv_test_result AS DNAPCRResult,
    TIMESTAMPDIFF(DAY, lab.dna_pcr_sample_collection_date, lab.date_dbs_result_received) AS TAT,
    lab.reason_sample_rejected_or_test_not_done AS Reason,
    cpt_start.CPTStartDate AS CPTStartDate,
    cpt_start.CPTDose AS CPTDose,
    fn_gregorian_to_ethiopian_calendar(cpt_stop.CPTDiscontinuedDate, 'D/M/Y') AS CPTDiscontinuedDate,
    CASE
        WHEN f.no_clinical_or_laboratory_evidence_of_hiv = 'Yes' THEN 'No Clinical or laboratory evidence of HIV'
        WHEN f.clinical_evidence_of_hiv = 'Yes' THEN 'Clinical evidence of HIV'
        WHEN f.laboratory_evidence_of_hiv_dna_pcr_antibody_tests = 'Yes' THEN 'Laboratory evidence of HIV(DNA PCR Test)'
        ELSE ''
    END AS Conclusion,
    CASE
        WHEN f.continue_to_followup = 'Yes' THEN 'Continue follow-up – Still on BF/Exposed'
        WHEN f.transferred_out = 'Yes' THEN 'TO'
        WHEN f.lost_to_followup = 'Yes' THEN 'Lost to follow up'
        WHEN f.died = 'Yes' THEN 'Died'
        WHEN f.referred_for_pediatric_hiv_care_within_facility = 'Yes' THEN 'Positive - Referred for Pediatric HIV care within facility'
        WHEN f.referred_pediatric_hiv_care_outside_facility = 'Yes' THEN 'Positive - Referred for Pediatric HIV care outside facility'
        WHEN f.discharged_negative_form_care_hiv_free = 'Yes' THEN 'Discharged Negative from care (HIV Free)'
        ELSE f.decision
    END AS Decision,
    f.next_visit_date AS NextVisitDate,
    fo.hei_pmtct_final_outcome AS HEIPMTCTFinalOutcome,
    fo.date_when_final_outcome_was_known AS DateofFinalOutcome,
    fn_gregorian_to_ethiopian_calendar(fo.date_when_final_outcome_was_known, 'D/M/Y') AS DateofFinalOutcomeET,
    imm.Immunizations AS IMMUNIZATION,
    CASE
        WHEN f.next_visit_date < REPORT_END_DATE AND fv.client_id IS NULL THEN TIMESTAMPDIFF(DAY, f.next_visit_date, REPORT_END_DATE)
        ELSE NULL
    END AS Missed_Days,
    fo.name_of_where_patient_was_referred_to AS ReferredTo,
    TIMESTAMPDIFF(MONTH, client.date_of_birth, lab.dna_pcr_sample_collection_date) AS AgeSampleCollectionMonth,
    TIMESTAMPDIFF(WEEK, client.date_of_birth, lab.dna_pcr_sample_collection_date) AS AgeSampleCollectionInWeeks,
    TIMESTAMPDIFF(YEAR, client.date_of_birth, lab.dna_pcr_sample_collection_date) AS AgeSampleCollectionYear

FROM HEI_FollowUp f
JOIN mamba_dim_client client ON f.client_id = client.client_id
LEFT JOIN HEI_Enrollment enr ON f.client_id = enr.client_id AND enr.row_num = 1
LEFT JOIN HEI_Lab lab ON f.client_id = lab.client_id AND lab.row_num = 1
LEFT JOIN Immunization imm ON f.client_id = imm.client_id
LEFT JOIN CPT_Start cpt_start ON f.client_id = cpt_start.client_id
LEFT JOIN CPT_Stop cpt_stop ON f.client_id = cpt_stop.client_id
LEFT JOIN FinalOutcome fo ON f.client_id = fo.client_id AND fo.row_num = 1
LEFT JOIN Future_Visit fv ON f.client_id = fv.client_id
WHERE f.row_num = 1;
