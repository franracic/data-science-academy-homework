CREATE TABLE IF NOT EXISTS fracic.daily_entity_follows
(
    event_date String,
    entity     String,
    name       String,
    count      SimpleAggregateFunction(sum, UInt64)
)
    ENGINE = AggregatingMergeTree()
        ORDER BY (event_date, entity, name);

CREATE MATERIALIZED VIEW IF NOT EXISTS fracic.mv_daily_entity_follows
    TO fracic.daily_entity_follows AS
SELECT event_date,
       replaceOne(event_name, 'follow_', '') AS entity,
       multiIf(
               event_name = 'follow_team',
               dictGet('fracic.team_dictionary', 'name', toUInt64(id)),
               event_name = 'follow_player',
               dictGet('fracic.player_dictionary', 'name', toUInt64(id)),
               event_name = 'follow_league',
               dictGet('fracic.uniquetournament_dictionary', 'name', toUInt64(id)),
               ''
       )                                     AS name,
       count()                               AS count
FROM bq.events
WHERE event_name IN ('follow_team', 'follow_player', 'follow_league')
GROUP BY event_date, entity, name;

INSERT INTO fracic.daily_entity_follows
SELECT event_date,
       replaceOne(event_name, 'follow_', '') AS entity,
       multiIf(
               event_name = 'follow_team',
               dictGet('fracic.team_dictionary', 'name', toUInt64(id)),
               event_name = 'follow_player',
               dictGet('fracic.player_dictionary', 'name', toUInt64(id)),
               event_name = 'follow_league',
               dictGet('fracic.uniquetournament_dictionary', 'name', toUInt64(id)),
               ''
       )                                     AS name,
       count()                               AS count
FROM bq.events
WHERE event_name IN ('follow_team', 'follow_player', 'follow_league')
GROUP BY event_date, entity, name;
