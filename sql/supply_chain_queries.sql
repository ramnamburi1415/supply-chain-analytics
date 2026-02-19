-- Query 1: Overall KPI Summary
SELECT
    COUNT(shipment_id) AS total_shipments,
    ROUND(SUM(CASE WHEN delivery_status IN ('On Time','Early') THEN 1.0 ELSE 0 END)
        / COUNT(shipment_id) * 100, 2) AS on_time_pct,
    ROUND(AVG(shipment_cost_usd), 2) AS avg_cost_usd,
    ROUND(SUM(shipment_cost_usd), 2) AS total_spend_usd,
    SUM(CASE WHEN delivery_status = 'Late' THEN 1 ELSE 0 END) AS late_count
FROM shipments
WHERE delivery_status != 'Cancelled';

-- Query 2: Warehouse Performance
SELECT w.warehouse_name, w.region,
    COUNT(s.shipment_id) AS total_shipments,
    ROUND(SUM(CASE WHEN s.delivery_status IN ('On Time','Early') THEN 1.0 ELSE 0 END)
        / COUNT(s.shipment_id) * 100, 2) AS on_time_pct,
    ROUND(AVG(s.shipment_cost_usd), 2) AS avg_cost_usd
FROM warehouses w
JOIN shipments s ON w.warehouse_id = s.warehouse_id
WHERE s.delivery_status != 'Cancelled'
GROUP BY w.warehouse_id, w.warehouse_name, w.region
ORDER BY on_time_pct DESC;

-- Query 3: Carrier Benchmarking
SELECT carrier,
    COUNT(shipment_id) AS total_shipments,
    ROUND(SUM(CASE WHEN delivery_status IN ('On Time','Early') THEN 1.0 ELSE 0 END)
        / COUNT(shipment_id) * 100, 2) AS on_time_pct,
    ROUND(AVG(shipment_cost_usd), 2) AS avg_cost_usd,
    SUM(CASE WHEN delivery_status = 'Late' THEN 1 ELSE 0 END) AS late_count
FROM shipments
WHERE delivery_status != 'Cancelled'
GROUP BY carrier
ORDER BY on_time_pct DESC;

-- Query 4: Supplier Reliability (4-table JOIN)
SELECT sup.supplier_name, sup.country,
    COUNT(s.shipment_id) AS total_shipments,
    ROUND(SUM(CASE WHEN s.delivery_status IN ('On Time','Early') THEN 1.0 ELSE 0 END)
        / COUNT(s.shipment_id) * 100, 2) AS actual_on_time_pct,
    ROUND(sup.reliability_score * 100, 1) AS self_reported_pct
FROM suppliers sup
JOIN products p  ON sup.supplier_id = p.supplier_id
JOIN orders o    ON p.product_id = o.product_id
JOIN shipments s ON o.order_id = s.order_id
WHERE s.delivery_status != 'Cancelled'
GROUP BY sup.supplier_id, sup.supplier_name, sup.country, sup.reliability_score
HAVING COUNT(s.shipment_id) > 100
ORDER BY actual_on_time_pct DESC
LIMIT 10;

-- Query 5: Order Priority vs Performance
SELECT o.order_priority,
    COUNT(o.order_id) AS total_orders,
    ROUND(SUM(CASE WHEN s.delivery_status IN ('On Time','Early') THEN 1.0 ELSE 0 END)
        / COUNT(o.order_id) * 100, 2) AS on_time_pct,
    ROUND(AVG(s.shipment_cost_usd), 2) AS avg_cost_usd
FROM orders o
JOIN shipments s ON o.order_id = s.order_id
WHERE s.delivery_status != 'Cancelled'
GROUP BY o.order_priority
ORDER BY FIELD(o.order_priority,'Critical','High','Medium','Low');
