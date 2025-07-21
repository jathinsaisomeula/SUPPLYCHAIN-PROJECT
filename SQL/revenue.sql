SELECT 
 `Product type`,
  `Price`, 
  COUNT (`Number of products sold`) AS sold_products_count,
   SUM (`Revenue generated`) AS total_revenue
 FROM `sacred-pipe-454410-p7.supply_chain.jathin_stores` 
GROUP BY 
  `Product type`, 
   `Price`
ORDER BY 
  total_revenue DESC;