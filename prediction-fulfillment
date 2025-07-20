select
      distinct date,
      sum(revenu) over(order by date) as revenu_date,
      sum(predict) over(order by date) as predict_date,
      sum(revenu) over(order by date) /
      sum(predict) over(order by date) * 100 as persent_revenu_from_predict
from
(
  select
      s.date,
      sum(p.price) as revenu,
      0 as predict


from `data-analytics-mate.DA.order` o
join `data-analytics-mate.DA.session` s
on o.ga_session_id = s.ga_session_id
join `data-analytics-mate.DA.product` p
on o.item_id = p.item_id
group by s.date


union all


select
      date,
      0 as revenu,
      predict
from `data-analytics-mate.DA.revenue_predict`
) revenu_predict
order by date
