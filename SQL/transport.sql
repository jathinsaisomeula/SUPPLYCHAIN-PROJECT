SELECT 
  AVG(`Shipping times`) AS avg_shipping_time ,
   `Shipping carriers` ,
   SUM(`Shipping costs`) AS total_shippinng_cost, 
   `Supplier name`,
   `Transportation modes`
 FROM `sacred-pipe-454410-p7.supply_chain.jathin_stores` 
 GROUP BY
   `Supplier name`,
    `Shipping carriers` ,
   `Transportation modes` ;