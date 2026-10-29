SELECT
    -- customers_t columns
    c.customer_id,
    c.first_name AS customer_first_name,
    c.last_name AS customer_last_name,
    c.email AS customer_email,
    c.phone AS customer_phone,
    c.city AS customer_city,
    c.province AS customer_province,
    c.country AS customer_country,
    c.created_timestamp AS customer_created_timestamp,
    c.updated_timestamp AS customer_updated_timestamp,
    c.is_active AS customer_is_active,
    c.processed_at AS customer_processed_at,

    -- orders_t columns (excluding customer_id which is already from customers)
    o.order_id,
    o.store_id,
    o.order_timestamp,
    o.payment_method,
    o.order_status,
    o.total_amount,
    o.created_timestamp AS order_created_timestamp,
    o.updated_timestamp AS order_updated_timestamp,
    o.is_active AS order_is_active,
    o.processed_at AS order_processed_at,

    -- order_items_t columns (excluding order_id and product_id which come from other tables)
    oi.order_item_id,
    oi.quantity,
    oi.unit_price,
    oi.line_amount,
    oi.created_timestamp AS order_item_created_timestamp,
    oi.updated_timestamp AS order_item_updated_timestamp,
    oi.is_active AS order_item_is_active,
    oi.processed_at AS order_item_processed_at,

    -- products_t columns
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    p.price,
    p.created_timestamp AS product_created_timestamp,
    p.updated_timestamp AS product_updated_timestamp,
    p.is_active AS product_is_active,
    p.processed_at AS product_processed_at,

    -- stores_t columns (excluding store_id which is already from orders)
    s.store_name,
    s.city AS store_city,
    s.province AS store_province,
    s.country AS store_country,
    s.created_timestamp AS store_created_timestamp,
    s.updated_timestamp AS store_updated_timestamp,
    s.is_active AS store_is_active,
    s.processed_at AS store_processed_at,

    -- employees_t columns (excluding store_id which is already from orders)
    e.employee_id,
    e.first_name AS employee_first_name,
    e.last_name AS employee_last_name,
    e.email AS employee_email,
    e.job_title,
    e.salary,
    e.created_timestamp AS employee_created_timestamp,
    e.updated_timestamp AS employee_updated_timestamp,
    e.is_active AS employee_is_active,
    e.processed_at AS employee_processed_at

FROM walmart.silver_t.orders_t o
LEFT JOIN walmart.silver_t.customers_t c
    ON o.customer_id = c.customer_id
LEFT JOIN walmart.silver_t.order_items_t oi
    ON o.order_id = oi.order_id
LEFT JOIN walmart.silver_t.products_t p
    ON oi.product_id = p.product_id
LEFT JOIN walmart.silver_t.stores_t s
    ON o.store_id = s.store_id
LEFT JOIN walmart.silver_t.employees_t e
    ON o.store_id = e.store_id