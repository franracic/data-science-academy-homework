SELECT COUNT(DISTINCT user_pseudo_id) AS broj_korisnika
FROM bq.events
WHERE event_name = 'drawer_action'
  AND item_name = 'Buzzer Feed'