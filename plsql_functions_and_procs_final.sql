/*Insert Entry stored procedure*/
CREATE OR REPLACE PROCEDURE insert_entry(i_load_id NUMBER, i_file_id NUMBER , i_fund_id VARCHAR2, i_as_of_date DATE, i_nav_val NUMBER, i_mdm_reg_id NUMBER, i_update_id VARCHAR2)
IS
  entry STG_NAV%ROWTYPE;
BEGIN
  SELECT * INTO entry FROM STG_NAV WHERE LOAD_ID = i_load_id AND AS_OF_DATE LIKE i_as_of_date AND FILE_ID = i_file_id;
  UPDATE STG_NAV SET ROW = entry WHERE LOAD_ID = i_load_id AND AS_OF_DATE = i_as_of_date AND FILE_ID = i_file_id;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
    INSERT INTO STG_NAV(LOAD_ID, FILE_ID, FUND_ID, AS_OF_DATE, NAV_VAL, MDM_REG_ID, PROCESSED_FG, VALIDATION_FG, UPDATE_ID, UPDATE_TMSTMP) VALUES(i_load_id, i_file_id, i_fund_id, i_as_of_date, i_nav_val, i_mdm_reg_id, 'N', 'N', i_update_id, CURRENT_TIMESTAMP);
END;
/

/*Validate Alpha Numeric fiels (Check for NULLS) stored procedure */
CREATE OR REPLACE PROCEDURE validate_entries_alpha(i_file_id NUMBER, null_status_message_alphanumeric OUT VARCHAR2)
IS
  null_total NUMBER := 0;
  total_row_count NUMBER;
  error_threshold NUMBER;
  validaiton_flag_updated_status VARCHAR(10);
BEGIN
  SELECT COUNT(*) INTO total_row_count FROM STG_NAV WHERE FILE_ID = i_file_id AND PROCESSED_FG NOT LIKE 'Y';
  IF total_row_count = 0 THEN
    null_status_message_alphanumeric := 'Nothing to process.';
  ELSE
    error_threshold := total_row_count * 0.1;
    
    SELECT COUNT(*) INTO null_total FROM STG_NAV WHERE FILE_ID = i_file_id AND (FUND_ID IS NULL OR UPDATE_ID IS NULL) AND PROCESSED_FG NOT LIKE 'Y';
    
    null_status_message_alphanumeric := get_null_status_message(null_total, error_threshold);
    validaiton_flag_updated_status := set_validation_flag_null_check(i_file_id);
  END IF;
END;
/

/* Validate Numeric fields (Check for NULLS/zeros) stored procedure */
CREATE OR REPLACE PROCEDURE validate_entries_numeric(i_file_id NUMBER, null_status_message_numeric OUT VARCHAR2)
IS
  null_total NUMBER := 0;
  total_row_count NUMBER;
  error_threshold NUMBER;
  validaiton_flag_updated_status VARCHAR(10);
BEGIN
  SELECT COUNT(*) INTO total_row_count FROM STG_NAV WHERE FILE_ID = i_file_id AND PROCESSED_FG NOT LIKE 'Y';
  IF total_row_count = 0 THEN
    null_status_message_numeric := 'Nothing to process.';
  ELSE
    error_threshold := total_row_count * 0.1;
    
    SELECT COUNT(*) INTO null_total FROM STG_NAV WHERE FILE_ID = i_file_id AND (MDM_REG_ID = 0 OR NAV_VAL = 0) AND PROCESSED_FG NOT LIKE 'Y';
    
    null_status_message_numeric := get_null_status_message(null_total, error_threshold);
    validaiton_flag_updated_status := set_validation_flag_null_check(i_file_id);
  END IF;
END;
/

/* Get NULL status message function */
CREATE OR REPLACE FUNCTION get_null_status_message(null_total NUMBER, error_threshold NUMBER) RETURN VARCHAR2
IS
  null_status_message VARCHAR2(50);
BEGIN
  /*Check for the error threshold, raise the warning message if the errors are below 10% of the total rows, errors if above 10%, else success*/ 
  IF null_total > error_threshold THEN
    null_status_message := 'Error Nulls: 1 or more values in Error';
  ELSIF null_total < error_threshold AND null_total > 0 THEN
    null_status_message := 'Warning Nulls: Null values present';
  ELSE
    null_status_message := 'Success';
  END IF;
  RETURN null_status_message;
END;
/

/* Set validation flag function */
CREATE OR REPLACE FUNCTION set_validation_flag_null_check(i_file_id NUMBER) RETURN VARCHAR2
IS
  validaiton_flag_updated VARCHAR(10);
BEGIN
  /*Set the validation flag to either 'N' or 'Y'*/
  UPDATE STG_NAV SET VALIDATION_FG = 'N'
  WHERE FILE_ID = i_file_id AND (FUND_ID IS NULL OR UPDATE_ID IS NULL OR MDM_REG_ID = 0 OR NAV_VAL = 0) AND PROCESSED_FG NOT LIKE 'Y';
  UPDATE STG_NAV SET VALIDATION_FG = 'Y'
  WHERE FILE_ID = i_file_id AND (FUND_ID IS NOT NULL AND UPDATE_ID IS NOT NULL AND MDM_REG_ID != 0 AND NAV_VAL != 0) AND PROCESSED_FG NOT LIKE 'Y';
  validaiton_flag_updated := 'Success';
  RETURN validaiton_flag_updated;
END;
/

/* Find Variance stored procedure */
CREATE OR REPLACE PROCEDURE find_variance_sp(i_file_id NUMBER, variance_status_message OUT VARCHAR2)
IS
  nav_val_variance NUMBER := 0;
  current_nav_val NUMBER;
  previous_nav_val NUMBER := 0;
  nav_val_mean NUMBER;
  previous_load_id NUMBER := 0;
  /* Variable to check the number to divide by  for mean*/
  total_load_ids NUMBER;
  has_warning BOOLEAN := FALSE;
  loop_count NUMBER := 0;
BEGIN
  FOR i IN (SELECT NAV_VAL, LOAD_ID, AS_OF_DATE FROM STG_NAV WHERE FILE_ID = i_file_id AND VALIDATION_FG NOT LIKE 'N' AND PROCESSED_FG NOT LIKE 'Y' ORDER BY FILE_ID, LOAD_ID, AS_OF_DATE) LOOP
    loop_count := loop_count + 1;
    /*Check to see if you have gotten a new LOAD_ID for the set of days which will signal the old nav val needs to be zero */
    IF previous_load_id != i.LOAD_ID THEN
      previous_nav_val := 0;
      total_load_ids := 1;
    ELSE
      total_load_ids := 2;
    END IF;
  
    current_nav_val := i.NAV_VAL;
    nav_val_mean := (previous_nav_val + current_nav_val) / total_load_ids;
    IF previous_nav_val = 0 THEN
      nav_val_variance := 0;
    ELSE
      nav_val_variance := ( POWER((current_nav_val - nav_val_mean), 2) + POWER((previous_nav_val - nav_val_mean), 2) ) / 2;
    END IF;
    previous_nav_val := current_nav_val;
    
    /*DBMS_OUTPUT.PUT_LINE('Variance ' || nav_val_variance);*/
    IF nav_val_variance > 0.01 THEN
      UPDATE STG_NAV SET VALIDATION_FG = 'N'
      WHERE FILE_ID = i_file_id AND LOAD_ID = I.LOAD_ID AND AS_OF_DATE = I.AS_OF_DATE;
      variance_status_message := 'Error Variance: 1 or more values in Error';
    ELSIF nav_val_variance > 0 THEN
      has_warning := TRUE;
    END IF;
    previous_load_id := i.LOAD_ID;
  END LOOP;
  IF loop_count = 0 THEN
    variance_status_message := 'Nothing to process.';
  ELSIF variance_status_message IS NULL AND has_warning THEN
    variance_status_message := 'Warning Variance: variance over 1%';
  ELSIF variance_status_message IS NULL THEN
    variance_status_message := 'Success';
  END IF;
END;
/

/* Transfer the valid entries to the NAV table stored procedure */
CREATE OR REPLACE PROCEDURE transfer_from_stage
IS
BEGIN
  INSERT INTO NAV(LOAD_ID, FILE_ID, FUND_ID, AS_OF_DATE, NAV_VAL, MDM_REG_ID, PROCESSED_FG, VALIDATION_FG, UPDATE_ID, UPDATE_TMSTMP)
  SELECT LOAD_ID, FILE_ID, FUND_ID, AS_OF_DATE, NAV_VAL, MDM_REG_ID, 'Y', 'Y', UPDATE_ID, UPDATE_TMSTMP FROM STG_NAV sn
  WHERE sn.VALIDATION_FG LIKE 'Y' AND sn.PROCESSED_FG LIKE 'N';
  
  UPDATE STG_NAV SET PROCESSED_FG = 'Y'
  WHERE VALIDATION_FG LIKE 'Y';
END;
/

/* Return the valid entries from the NAV table for the response */
CREATE OR REPLACE PROCEDURE get_valid_entries(i_file_id NUMBER, entry OUT SYS_REFCURSOR)
IS
BEGIN
  OPEN entry FOR SELECT * FROM NAV WHERE FILE_ID = i_file_id ORDER BY FILE_ID, LOAD_ID, AS_OF_DATE;
END;
/
