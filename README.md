# xldb
 Utility for loading data into databases using exel tables


# Test PostgreSQL

```sh

docker run -itd                      \
    -e POSTGRES_USER=user            \
    -e POSTGRES_PASSWORD=123456      \
    -e POSTGRES_DB=test              \
    -p 5454:5432                     \
    --name 'xldb_test'               \
    postgres

```

```sql

CREATE TABLE public.test_1 (
    id BIGSERIAL PRIMARY KEY,
    type VARCHAR(100) NOT NULL,
    name VARCHAR(200) NOT NULL,
    count INT,
    comm TEXT,
    sum REAL,
    nds NUMERIC,
    position DOUBLE precision,
    is_active BOOLEAN DEFAULT FALSE,
    creatdAt TIMESTAMPTZ NOT NULL
);
```