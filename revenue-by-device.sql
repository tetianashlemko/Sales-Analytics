WITH revenue_usd AS
(
    SELECT
         
          sp.continent,
          SUM(p.price) as revenue,
          SUM(CASE WHEN  device = 'mobile' THEN p.price end) AS revenue_from_mobile,
          SUM(CASE WHEN  device = 'desktop' THEN p.price end) AS revenue_from_desktop,
         
    FROM `DA.order` o
    JOIN `DA.product` p
    ON o.item_id = p.item_id
    JOIN `DA.session_params` sp
    ON o.ga_session_id = sp.ga_session_id
    GROUP BY sp.continent
),
 
accounts AS
(
    SELECT
          sp.continent as continent,
          COUNT(acs.account_id) AS account_count,
          COUNT(CASE WHEN is_verified = 1 THEN ac.id END) AS verified_account_cnt,
          COUNT(sp.ga_session_id) AS session_count
    FROM `DA.session_params` sp
    LEFT JOIN `DA.account_session` acs
    ON sp.ga_session_id = acs.ga_session_id
    LEFT JOIN  `DA.account` ac
    ON acs.account_id = ac.id
    GROUP BY sp.continent
)
      SELECT
            accounts.continent,
            revenue_usd.revenue,
            revenue_usd.revenue_from_mobile,
            revenue_usd.revenue_from_desktop,
            sum(revenue_usd.revenue) OVER (partition by revenue_usd.continent)
                / sum(revenue_usd.revenue) OVER () *100 as percent_revenue_from_total,

            accounts.account_count,
            accounts.verified_account_cnt,
            accounts.session_count
               
      FROM accounts
      LEFT JOIN revenue_usd
      ON accounts.continent = revenue_usd.continent
      ORDER BY revenue DESC;
