# Підрахунок кількості унікальних акаунтів у розрізі дати, країни, параметрів верифікації та підписки
WITH accounts AS
(
  SELECT
        s.date,
        sp.country,
        ac.send_interval,
        ac.is_verified,
        ac.is_unsubscribed,
        COUNT(distinct ac.id) AS account_cnt
  FROM `DA.account` ac
  JOIN `DA.account_session` acs ON ac.id = acs.account_id
  JOIN `DA.session_params` sp ON acs.ga_session_id = sp.ga_session_id
  JOIN `DA.session` s ON sp.ga_session_id = s.ga_session_id
  GROUP by s.date,
        sp.country,
        ac.send_interval,
        ac.is_verified,
        ac.is_unsubscribed


),


# Підрахунок кількості відправлених, відкритих та переглянутих повідомлень у розрізі дати та країни
msg_metrics AS
(
  SELECT
        DATE_ADD(s.date, INTERVAL es.sent_date DAY) as date,
        sp.country,
        ac.send_interval,
        ac.is_verified,
        ac.is_unsubscribed,


        COUNT(es.id_message) AS sent_msg,
        COUNT(eo.id_message) AS open_msg,
        COUNT(ev.id_message) as visit_msg,
       
  FROM `data-analytics-mate.DA.email_sent` es
  JOIN `data-analytics-mate.DA.account_session` acs on es.id_account = acs.account_id
  JOIN `data-analytics-mate.DA.session` s on acs.ga_session_id = s.ga_session_id
  LEFT JOIN `data-analytics-mate.DA.email_open` eo ON es.id_message = eo.id_message
  LEFT JOIN `data-analytics-mate.DA.email_visit` ev ON es.id_message = ev.id_message
  JOIN `DA.session_params` sp ON s.ga_session_id = sp.ga_session_id
  JOIN `DA.account` ac ON es.id_account = ac.id
  GROUP BY DATE_ADD(s.date, INTERVAL es.sent_date DAY),
                  sp.country,
            ac.send_interval,
            ac.is_verified,
            ac.is_unsubscribed    
),


# Об'єднання двох попередніх метрики, замінюючи відсутні значення на 0

  unions AS
(
  SELECT
        date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        0 as sent_msg,
        0 as open_msg,
        0 as visit_msg,
        account_cnt
  FROM accounts


      UNION ALL


  SELECT
        date,
        country,
        send_interval,
        is_verified,
        is_unsubscribed,
        sent_msg,
        open_msg,
        visit_msg,
        0 as account_cnt


  FROM msg_metrics


),


#Агрегація даних, та групування по спільних колонках
all_metrics AS
(
     SELECT
            date,
            country,
            send_interval,
            is_verified,
            is_unsubscribed,
            SUM(account_cnt) as account_cnt,
            SUM(sent_msg) as sent_msg,
            SUM(open_msg) as open_msg,
            SUM(visit_msg) as visit_msg,
           
      FROM unions
      GROUP BY date,
            country,
            send_interval,
            is_verified,
            is_unsubscribed
),


#Розрахунок загальної кількості акаунтів та повідомлень у розрізі країни
final as
(    
      SELECT
            date,
            country,
            send_interval,
            is_verified,
            is_unsubscribed,
            account_cnt,
            sent_msg,
            open_msg,
            visit_msg,


            SUM(account_cnt) OVER (partition by country) as total_country_account_cnt,
            SUM(sent_msg) OVER (partition by country) as total_country_sent_cnt


       FROM all_metrics
)   
 
# Визначення рейтингу країн за кількістю акаунтів та відправлених повідомлень (без пропусків у рангах)
# Вибір тільки топ-10 країн
SELECT
      date,
      country,
      send_interval,
      is_verified,
      is_unsubscribed,
      account_cnt,
      sent_msg,
      open_msg,
      visit_msg,
      total_country_account_cnt,
      total_country_sent_cnt,


      rank_total_country_account_cnt,
      rank_total_country_sent_cnt


FROM(


            SELECT
                  date,
                  country,
                  send_interval,
                  is_verified,
                  is_unsubscribed,
                  account_cnt,
                  sent_msg,
                  open_msg,
                  visit_msg,
                  total_country_account_cnt,
                  total_country_sent_cnt,


                  DENSE_RANK() OVER (ORDER BY total_country_account_cnt DESC) AS rank_total_country_account_cnt,
                  DENSE_RANK() OVER (ORDER BY total_country_sent_cnt DESC) AS rank_total_country_sent_cnt


            FROM final) LAST
           
WHERE rank_total_country_account_cnt <= 10 OR rank_total_country_sent_cnt <= 10


ORDER BY date, rank_total_country_account_cnt;


