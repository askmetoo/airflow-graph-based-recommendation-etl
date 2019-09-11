CREATE TABLE user_recs (
    user_id     int,
    rec_id      int,
    weight      int NOT NULL,
    created_at  timestamp NOT NULL DEFAULT NOW()
    PRIMARY KEY (user_id, rec_id)
);

CREATE TABLE user_rec_reasons (
    user_id     int NOT NULL,
    rec_id      int NOT NULL,
    reason_id   int NOT NULL,
);

CREATE TABLE room_recs (
    user_id int,
    rec_id  int,
    weight  int NOT NULL,
    created_at  timestamp NOT NULL DEFAULT NOW()
    PRIMARY KEY (user_id, rec_id)
);

CREATE TABLE room_rec_reasons (
    user_id     int NOT NULL,
    rec_id      int NOT NULL,
    reason_id   int NOT NULL,
);

CREATE TABLE users (
    id                  int PRIMARY KEY,
    username            varchar NOT NULL,
    experience          varchar, 
    approach            varchar,
    holding_period      varchar,
    bonds               smallint,
    equities            smallint, 
    forex               smallint,
    futures             smallint,
    options             smallint,
    private_companies   smallint
);

CREATE TABLE rooms (
    id                  INT PRIMARY KEY
    slug                varchar NOT NULL
    biotechnology       smallint,
    cryptocurrencies    smallint,
    day_trading         smallint,
    etfs                smallint,
    education           smallint,
    energy              smallint,
    equities            smallint,
    forex               smallint,
    fundamentals        smallint,
    futures             smallint,
    global_macro        smallint,
    green_energy        smallint,
    long_term_investing smallint,
    momentum_trading    smallint,
    news                smallint,
    non_market_talk     smallint,
    options             smallint,
    personal_finance    smallint,
    precious_metals     smallint,
    quant               smallint,
    swing_trading       smallint,    
    technicals          smallint,
    technology          smallint
);