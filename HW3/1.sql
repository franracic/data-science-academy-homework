CREATE OR REPLACE DICTIONARY fracic.event_dictionary
(
    id            Int32,
    sport_id      Int32,
    tournament_id Int32,
    hometeam_id   Int32,
    awayteam_id   Int32
)
    PRIMARY KEY id
    SOURCE (CLICKHOUSE(db 'sports' table 'event' user 'fracic' password 'NZPwB7LR47x6E0RqKw5q'))
    LAYOUT (HASHED())
    LIFETIME ( 500 );

CREATE OR REPLACE DICTIONARY fracic.sport_dictionary
(
    id   UInt64,
    name String
)
    PRIMARY KEY id
    SOURCE (CLICKHOUSE(db 'sports' table 'sport' user 'fracic' password 'NZPwB7LR47x6E0RqKw5q'))
    LAYOUT (FLAT())
    LIFETIME ( 500 );

CREATE OR REPLACE DICTIONARY fracic.tournament_dictionary
(
    id                  Int32,
    name                String,
    uniquetournament_id Int32
)
    PRIMARY KEY id
    SOURCE (CLICKHOUSE(db 'sports' table 'tournament' user 'fracic' password 'NZPwB7LR47x6E0RqKw5q'))
    LAYOUT (HASHED())
    LIFETIME ( 500 );

CREATE OR REPLACE DICTIONARY fracic.uniquetournament_dictionary
(
    id   Int32,
    name String
)
    PRIMARY KEY id
    SOURCE (CLICKHOUSE(db 'sports' table 'uniquetournament' user 'fracic' password 'NZPwB7LR47x6E0RqKw5q'))
    LAYOUT (HASHED())
    LIFETIME ( 500 );

CREATE OR REPLACE DICTIONARY fracic.team_dictionary
(
    id       Int32,
    name     String,
    sport_id Int32
)
    PRIMARY KEY id
    SOURCE (CLICKHOUSE(db 'sports' table 'team' user 'fracic' password 'NZPwB7LR47x6E0RqKw5q'))
    LAYOUT (SPARSE_HASHED())
    LIFETIME ( 500 );

CREATE OR REPLACE DICTIONARY fracic.player_dictionary
(
    id      Int32,
    name    String,
    team_id Int32
)
    PRIMARY KEY id
    SOURCE (CLICKHOUSE(db 'sports' table 'player' user 'fracic' password 'NZPwB7LR47x6E0RqKw5q'))
    LAYOUT (SPARSE_HASHED())
    LIFETIME ( 500 );