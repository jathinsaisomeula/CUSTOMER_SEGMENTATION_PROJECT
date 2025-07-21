SELECT
  CITY,
  STATE,
  SUM(`REVENUE GENERATED`) AS total_revenue 
FROM `sacred-pipe-454410-p7.customer_segmentation.jathin_markt`
GROUP BY
  CITY ,
  STATE 
ORDER BY
  total_revenue DESC; 