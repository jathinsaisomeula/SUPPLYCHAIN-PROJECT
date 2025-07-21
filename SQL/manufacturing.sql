SELECT 
 `Product type` ,
  SUM(`Production volumes`) AS total_production , 
  `Manufacturing lead time`,
  SUM(`Manufacturing costs`) AS total_manufacturing_costs , 
   `Defect rates`
 FROM `sacred-pipe-454410-p7.supply_chain.jathin_stores` 
 GROUP BY
  `Product type` ,
   `Manufacturing lead time` ,
   `Defect rates` 
ORDER BY 
 total_manufacturing_costs DESC ;
  