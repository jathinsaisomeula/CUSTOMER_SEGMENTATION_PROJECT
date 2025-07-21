SELECT
  CAST(`DATE OF SALE` AS DATE) AS sale_date,
  CAST(`DELIVERY_DATE_TIME` AS DATE) AS delivery_date,
  DATETIME_DIFF(
    CAST(`DELIVERY_DATE_TIME` AS DATETIME), -- Cast to DATETIME for DATETIME_DIFF
    CAST(`DATE OF SALE` AS DATETIME),       -- Cast to DATETIME for DATETIME_DIFF
    DAY                                    -- Specify the unit (e.g., HOUR, MINUTE, DAY)
  ) AS delivery_time_in_days -- Renamed for clarity to include units
FROM `sacred-pipe-454410-p7.customer_segmentation.jathin_markt`;