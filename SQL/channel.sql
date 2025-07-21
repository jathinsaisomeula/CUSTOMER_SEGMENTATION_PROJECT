SELECT 
 COUNT (`CUSTOMER ID`) AS customer_count ,
 `MARKETING CHANNEL` ,
  GENDER 
 FROM `sacred-pipe-454410-p7.customer_segmentation.jathin_markt` 
 WHERE GENDER IN ("Male" , "Female")
 GROUP BY
  `MARKETING CHANNEL` , 
   GENDER ;