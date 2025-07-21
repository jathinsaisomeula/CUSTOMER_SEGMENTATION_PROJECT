SELECT 
 `DELIVERY MODE`,
 AVG (`DELIVERY_RATING`) AS avg_delivery_rating 
 FROM `sacred-pipe-454410-p7.customer_segmentation.jathin_markt` 
 GROUP BY
  `DELIVERY MODE` ;