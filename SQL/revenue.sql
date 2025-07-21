SELECT
  `PRODUCT TYPE`, -- Enclosed in backticks
  AVG(PRICE) AS avg_price,
  COUNT(AVALIABILITY) AS quantity_available, -- Corrected alias for clarity
  SUM(`REVENUE GENERATED`) AS total_revenue -- Enclosed in backticks
FROM `sacred-pipe-454410-p7.customer_segmentation.jathin_markt`
GROUP BY
  `PRODUCT TYPE` -- Enclosed in backticks
ORDER BY
  total_revenue DESC; -- Semicolon moved to the end