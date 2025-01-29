CREATE TABLE df_cleaned_with_arr_freq AS (
  WITH cleaned_1 AS (
    SELECT
      id AS id,
      source_system.UPPER() AS source_system,
      title.UPPER() AS title,
      first_name.UPPER().REPLACE('MIG_ERROR_', '').REPLACE('NO_SHOW_', '').REPLACE('DUPLICATE_', '').REPLACE('-', ' ').REPLACE('''', '') AS first_name,
      middle_names.UPPER().REPLACE('MIG_ERROR_', '').REPLACE('NO_SHOW_', '').REPLACE('DUPLICATE_', '').REPLACE('-', ' ').REPLACE('''', '') AS middle_names,
      last_name.UPPER().REPLACE('MIG_ERROR_', '').REPLACE('NO_SHOW_', '').REPLACE('DUPLICATE_', '').REPLACE('-', ' ').REPLACE('''', '') AS last_name,
      crn.UPPER() AS crn,
      prison_number.UPPER() AS prison_number,
      defendant_id.UPPER() AS defendant_id,
      master_defendant_id.UPPER() AS master_defendant_id,
      date_of_birth AS date_of_birth,
      birth_place.UPPER() AS birth_place,
      birth_country.UPPER() AS birth_country,
      nationality.UPPER() AS nationality,
      sex.UPPER() AS sex,
      religion.UPPER() AS religion,
      sexual_orientation.UPPER() AS sexual_orientation,
      ethnicity.UPPER() AS ethnicity,
      version AS version,
      first_name_aliases.LIST_TRANSFORM(x -> x.UPPER()).LIST_TRANSFORM(
        x -> x.REPLACE('MIG_ERROR_', '').REPLACE('NO_SHOW_', '').REPLACE('DUPLICATE_', '').REPLACE('-', ' ').REPLACE('''', '')
      ) AS first_name_alias_arr,
      last_name_aliases.LIST_TRANSFORM(x -> x.UPPER()).LIST_TRANSFORM(
        x -> x.REPLACE('MIG_ERROR_', '').REPLACE('NO_SHOW_', '').REPLACE('DUPLICATE_', '').REPLACE('-', ' ').REPLACE('''', '')
      ) AS last_name_alias_arr,
      date_of_birth_aliases AS date_of_birth_arr,
      mobile_arr.LIST_TRANSFORM(x -> x.UPPER()).LIST_TRANSFORM(x -> x.REGEXP_REPLACE('\n', ' ', 'g')).LIST_TRANSFORM(x -> x.REGEXP_REPLACE('[^0-9]', '', 'g')).LIST_TRANSFORM(x -> x.SUBSTR(-10)) AS mobile_arr,
      email_arr.LIST_TRANSFORM(x -> x.UPPER()).LIST_TRANSFORM(x -> x.TRIM().NULLIF('')).LIST_FILTER(
        x -> x.REGEXP_MATCHES(
          '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        )
      ).NULLIF(ARRAY []) AS email_arr,
      postcodes.LIST_TRANSFORM(x -> x.UPPER()).LIST_TRANSFORM(x -> x.REGEXP_REPLACE('\s', '', 'g')).LIST_FILTER(x -> x not in ('NF11NF')).LIST_SORT() AS postcode_arr,
      cro_arr.LIST_TRANSFORM(x -> x.UPPER()).LIST_DISTINCT().LIST_FILTER(x -> x not in ('000000/00Z')).LIST_SORT() AS cro_arr,
      driver_license_number_arr.LIST_TRANSFORM(x -> x.UPPER()) AS driver_license_number_arr,
      national_insurance_number_arr.LIST_TRANSFORM(x -> x.UPPER()) AS national_insurance_number_arr,
      pnc_arr.LIST_TRANSFORM(x -> x.UPPER()).LIST_DISTINCT().LIST_SORT() AS pnc_arr,
      sentence_date_arr.LIST_FILTER(x -> x not in ('1970-01-01', '1990-01-01')) AS sentence_date_arr
    FROM
      df_raw
  ),
  cleaned_2 AS (
    SELECT
      *,
      CONCAT_WS(' ', first_name, middle_names, last_name).REGEXP_SPLIT_TO_ARRAY('\s+').LIST_FILTER(x -> LENGTH(x) >= 3) AS names_split,
      CASE
        WHEN array_length(names_split) >= 1 THEN names_split [1]
        ELSE NULL
      END AS name_1_std,
      CASE
        WHEN array_length(names_split) >= 3 THEN names_split [2]
        ELSE NULL
      END AS name_2_std,
      CASE
        WHEN array_length(names_split) >= 4 THEN names_split [3]
        ELSE NULL
      END AS name_3_std,
      names_split [-1] AS last_name_std,
      array_distinct(array_concat([name_1_std], first_name_alias_arr)) AS forename_std_arr,
      array_distinct(
        array_concat(names_split [-1:], last_name_alias_arr)
      ) AS last_name_std_arr,
      source_system AS source_dataset,
      CONCAT_WS(' ', name_1_std, last_name_std) AS first_and_last_name_std,
      postcode_arr.LIST_TRANSFORM(x -> SUBSTR(x, 1, LENGTH(x) - 3)).LIST_DISTINCT() AS postcode_outcode_arr,
      sentence_date_arr [-1] AS sentence_date_single,
      cro_arr [1] AS cro_single,
      pnc_arr [1] AS pnc_single
    FROM
      cleaned_1
  ),
  df_cleaned AS (
    SELECT
      id AS id,
      source_system AS source_system,
      name_1_std AS name_1_std,
      name_2_std AS name_2_std,
      name_3_std AS name_3_std,
      last_name_std AS last_name_std,
      first_and_last_name_std AS first_and_last_name_std,
      forename_std_arr AS forename_std_arr,
      last_name_std_arr AS last_name_std_arr,
      date_of_birth AS date_of_birth,
      date_of_birth_arr AS date_of_birth_arr,
      sentence_date_single AS sentence_date_single,
      sentence_date_arr AS sentence_date_arr,
      postcode_arr AS postcode_arr,
      postcode_outcode_arr AS postcode_outcode_arr,
      cro_single AS cro_single,
      pnc_single AS pnc_single,
      crn AS crn,
      prison_number AS prison_number,
      birth_place AS birth_place,
      birth_country AS birth_country,
      nationality AS nationality
    FROM
      cleaned_2
  ),
  exploded_postcode AS (
    SELECT
      id,
      unnest(postcode_arr) AS value
    FROM
      df_cleaned
  ),
  postcode_term_freq AS (
    SELECT
      value AS term,
      COUNT(*) AS count,
      COUNT(*) / SUM(COUNT(*)) OVER () AS percentage_term_freq
    FROM
      exploded_postcode
    GROUP BY
      term
    ORDER BY
      count DESC
  ),
  joined_postcode AS (
    SELECT
      e.id,
      e.value,
      COALESCE(tf.percentage_term_freq, 0) AS rel_freq
    FROM
      postcode_term_freq tf
      RIGHT JOIN exploded_postcode e ON e.value = tf.term
  ),
  agg_postcode AS (
    SELECT
      id,
      array_agg(
        struct_pack(value := value, rel_freq := rel_freq)
      ) AS postcode_arr_with_freq
    FROM
      joined_postcode
    GROUP BY
      id
  )
  SELECT
    df_cleaned.*,
    agg_postcode.postcode_arr_with_freq
  FROM
    agg_postcode
    RIGHT JOIN df_cleaned ON df_cleaned.id = agg_postcode.id
)