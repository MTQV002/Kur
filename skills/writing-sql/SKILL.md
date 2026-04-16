---
name: writing-sql
description: Best practices for generating correct PostgreSQL queries
---

# Writing SQL — Best Practices

## PostgreSQL-Specific Rules
- Use `DATE_TRUNC('month', column)` for monthly grouping
- Use `CURRENT_DATE` and `INTERVAL` for relative dates
- Use `COALESCE(col, 0)` for nullable numeric columns
- Use `::date`, `::numeric` for explicit casting
- Always alias subqueries

## Joining
- Always use explicit JOIN syntax (avoid commas in FROM)
- Prefer LEFT JOIN unless you need INNER JOIN
- Include ON clause with proper keys

## Aggregation
- Every non-aggregated column in SELECT must be in GROUP BY
- Use `FILTER (WHERE ...)` for conditional aggregation
- Prefer `COUNT(DISTINCT col)` over subqueries

## Common Patterns

### Revenue by Period
```sql
SELECT DATE_TRUNC('month', created_at) AS month,
       SUM(amount) AS total_revenue
FROM orders
WHERE created_at >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY 1
ORDER BY 1;
```

### Top N with Rank
```sql
SELECT name, revenue,
       RANK() OVER (ORDER BY revenue DESC) AS rank
FROM (
    SELECT c.name, SUM(o.amount) AS revenue
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    GROUP BY c.name
) sub
LIMIT 10;
```

### Year-over-Year Comparison
```sql
SELECT DATE_TRUNC('month', created_at) AS month,
       SUM(CASE WHEN EXTRACT(YEAR FROM created_at) = EXTRACT(YEAR FROM CURRENT_DATE) THEN amount END) AS this_year,
       SUM(CASE WHEN EXTRACT(YEAR FROM created_at) = EXTRACT(YEAR FROM CURRENT_DATE) - 1 THEN amount END) AS last_year
FROM orders
WHERE created_at >= CURRENT_DATE - INTERVAL '2 years'
GROUP BY 1
ORDER BY 1;
```

## Anti-Patterns to Avoid
- ❌ `SELECT *` — always specify columns
- ❌ Missing `GROUP BY` with aggregates
- ❌ Cartesian joins (missing WHERE/ON)
- ❌ Using `HAVING` where `WHERE` suffices
