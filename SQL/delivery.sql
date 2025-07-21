SELECT
  `DELIVERY_PERSON_NAME`, 
  AVG(`DELIVERY_RATING`) AS avg_delivery_rating,
  `DELIVERY MODE`
FROM `sacred-pipe-454410-p7.customer_segmentation.jathin_markt`
GROUP BY
  `DELIVERY MODE`,
  `DELIVERY_PERSON_NAME`
ORDER BY
  avg_delivery_rating DESC; 