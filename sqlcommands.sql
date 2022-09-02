CREATE TABLE wikitable
(
        id INTEGER Not NULL,
        title VARCHAR,
        body text
);
CREATE TABLE token_prev
(
        id serial Not NULL,
        word VARCHAR Not NULL,
        df INTEGER Not NULL
);
CREATE TABLE token
(
        id int Not NULL,
        word VARCHAR Not NULL,
        df INTEGER Not NULL
);
CREATE TABLE token_count
(
        docId  INTEGER Not NULL,
        tokenId  INTEGER Not NULL,
        tf INTEGER Not NULL
);

INSERT INTO token_prev (word, df)
SELECT
    word,
    ndoc
FROM
    ts_stat( 'select to_tsvector(''russian'', body) from wikitable' );

insert into token (id, word,df)
select id, word, df
from token_prev
where word similar to '[a-zA-Zа-яА-Я]+';

create index idx3 on token using btree(word);
INSERT INTO token_count (
    docid,
    tokenid,
    tf
)
select
    doc.id,
    tok.id,
    lex.total
from wikitable as doc
cross join lateral (
    select lexeme, cardinality(positions) as total
    from unnest(to_tsvector('russian', doc.body)) as tsvector
) as lex
inner join token as tok
    on tok.word = lex.lexeme
on conflict do nothing;

drop table token_prev;

alter table wikitable add column max_tf int;
alter table wikitable add column avg_tf real;
update wikitable w set max_tf = subquery.max_tf
FROM (

          SELECT docid, max(tf) as max_tf
          FROM token_count
          GROUP BY docid
    ) subquery
WHERE subquery.docid = w.id;
update wikitable w set avg_tf = subquery.avg_tf
FROM (

          SELECT docid, avg(tf) as avg_tf
          FROM token_count
          GROUP BY docid
    ) subquery
WHERE subquery.docid = w.id;

ALTER TABLE token ADD COLUMN t_idf real;
ALTER TABLE token ADD COLUMN p_idf real;
ALTER TABLE token ADD COLUMN bm25_idf real;
DO $$
DECLARE N integer;
BEGIN
    select count(*) from wikitable INTO N;

    UPDATE token SET t_idf = 1 + log(2, N/df);
END $$;
UPDATE token SET p_idf = 0;
DO $$
DECLARE N integer;
BEGIN
    select count(*) from wikitable INTO N;
    UPDATE token
    SET p_idf = 1 + log(2, (N::decimal-df)/df)
    WHERE df <> N;
END $$;
DO $$
DECLARE N integer;
BEGIN
    select count(*) from wikitable INTO N;

    UPDATE token SET bm25_idf = log(2, (N- df + 0.5)/(df+0.5));
END $$;

ALTER TABLE wikitable ADD COLUMN c_norm real;
alter table wikitable add column length int;
alter table wikitable add column bm25norm real;
alter table wikitable add column byte_size_norm real;
UPDATE wikitable SET byte_size_norm = 1 / power(octet_length(body), 0.375);
update wikitable w set c_norm = subquery.calculated_value
from (
        select docid, 1 / sqrt(sum(tf::decimal * tf)) as calculated_value
        from token_count
        group by docid
    ) subquery
where subquery.docid = w.id;
update wikitable w set length = subquery.calculated_value
from (
        select docid, count(*) as calculated_value
        from token_count
        group by docid
    ) subquery
where subquery.docid = w.id;
DO $$
DECLARE N real;
BEGIN
    select avg(length) from wikitable INTO N;

    UPDATE wikitable SET bm25norm = 2 * (0.25 + 0.75 * length / N);
END $$;

create index idx1 on token     using btree(id);
create index idx2 on wikitable using btree(id);




-- повторяющиеся слова в названии
select
    doc.id,
  doc.title,
    tok.id,
    lex.total
from wikitable as doc
cross join lateral (
    select lexeme, cardinality(positions) as total
    from unnest(to_tsvector('russian', doc.title)) as tsvector
) as lex
inner join token as tok
    on tok.word = lex.lexeme
where total > 1 and doc.title similar to '[a-zA-Zа-яА-Я ]+' limit 1000;




alter table wikitable add column norm_lt real;
alter table wikitable add column norm_ln real;
alter table wikitable add column norm_lp real;
alter table wikitable add column norm_nn real;
alter table wikitable add column norm_an real;
alter table wikitable add column norm_vn real;

update wikitable w set c_norm = subquery.calculated_value
from (
        select docid, 1 / sqrt(sum(tf::decimal * tf * t_idf * t_idf)) as calculated_value
        from token_count tc
        inner join token t on tc.tokenid = t.id
        inner join wikitable w on w.id = tc.docid
        group by docid
    ) subquery;
update wikitable w set norm_ln = subquery.calculated_value
from (
        select docid, 1 / sqrt(sum( (1+log(2,tc.tf))::decimal *  (1+log(2,tc.tf)))) as calculated_value
        from token_count tc
        group by docid
    ) subquery;
update wikitable w set norm_lp = subquery.calculated_value
from (
        select docid, 1 / sqrt(sum( t.p_idf * t.p_idf *(1+log(2,tc.tf))::decimal *  (1+log(2,tc.tf)))) as calculated_value
        from token_count tc
        inner join token t on tc.tokenid = t.id
        group by docid
    ) subquery;
update wikitable w set norm_nn = subquery.calculated_value
from (
        select docid, 1 / sqrt(sum(tf::decimal * tf)) as calculated_value
        from token_count tc
        inner join token t on tc.tokenid = t.id
        group by docid
    ) subquery;
update wikitable w set norm_an = subquery.calculated_value
from (
        select docid, 1 / sqrt(sum((0.5 + 0.5 * tc.tf / w.max_tf)::decimal * (0.5 + 0.5 * tc.tf / w.max_tf))) as calculated_value
        from token_count tc
        inner join token t on tc.tokenid = t.id
        inner join wikitable w on tc.docid = w.id
        group by docid
    ) subquery;
update wikitable w set norm_vn = subquery.calculated_value
from (
        select docid, 1 / sqrt(sum((1 + log(2, tf)) / (1+log(2, w.avg_tf::decimal))* (1 + log(2, tf)) / (1+log(2, w.avg_tf::decimal)))) as calculated_value
        from token_count tc
        inner join token t on tc.tokenid = t.id
        group by docid
    ) subquery;

-- функции по разным комбинациям однообразные, поэтому далее приведены только 2


CREATE OR REPLACE FUNCTION bm25(query_string text, categories varchar[], category_search boolean)
RETURNS table(
  title varchar, body text, score double precision
)
LANGUAGE PLPGSQL AS $$
DECLARE N integer;
begin
return query
with main_data AS (
  with query_data AS (
    select
      new_table.word,
      token_table.id
    from
      ts_stat(format('select to_tsvector(''russian'',  ''%1$I'')', query_string))   new_table
      left join token token_table on token_table.word = new_table.word
  )
  select
    w.title,
         w.body,
    tc.docid,
    t.word,
    t_idf as idf_weighted,
    w.c_norm as normalization,
    t.bm25_idf * 3 * (1+log(2,tc.tf)) / ((1+log(2,tc.tf)) + w.bm25norm)  as word_score
  from
    token_count tc
    inner join token t on t.id = tc.tokenid
    inner join wikitable w on w.id = tc.docid
    inner join query_data q on q.id = t.id
   WHERE
    CASE WHEN  category_search IS true
        THEN w.category = any(categories)
        ELSE TRUE
    END
  order by
    tc.docid
)
select
  main_data.title,
 main_data.body,
  (sum(
      main_data.word_score
  )) as score
from  main_data
where CHAR_LENGTH(main_data.body) > 2000
group by  main_data.title, main_data.body
order by  score desc;
end;
$$;

CREATE OR REPLACE FUNCTION lnnltc(query_string text, categories varchar[], category_search boolean)
RETURNS table(
  title varchar, body text, score double precision
)
LANGUAGE PLPGSQL AS $$
begin
return query
with main_query AS (
  with query_data AS (
    with c_norm AS (
      select  1 / sqrt(
          sum(log_tf :: decimal * log_tf)
        ) as query_c_norm
      from (
          select
            word,
            1 + log(2, nentry) as log_tf
          from
            ts_stat(format('select to_tsvector(''russian'',  ''%1$I'')', query_string))
        ) as t
    )
    select
      new_table.word,
      token_table.id,
      (t_idf * ( 1 + log(2, nentry)) * (
          select  query_c_norm from c_norm
        )
      ) as query_word_score
    from
      ts_stat(
           format('select to_tsvector(''russian'',  ''%1$I'')', query_string)
      ) new_table
      left join token token_table on token_table.word = new_table.word
  )
  select
    w.title,
         w.body,
    tc.docid,
    t.word,
    1+log(2,tc.tf) as tf_weighted,
    t_idf as idf_weighted,
    w.c_norm as normalization,
    (1+log(2,tc.tf))   as document_word_score,
    q.query_word_score
  from
    token_count tc
    inner join token t on t.id = tc.tokenid
    inner join wikitable w on w.id = tc.docid
    inner join query_data q on q.id = t.id
   WHERE
    CASE WHEN  category_search IS true
        THEN w.category = any(categories)
        ELSE TRUE
    END
  order by
    tc.docid
)
select
  main_query.title,
  main_query.body,
  (sum(
      main_query.query_word_score * main_query.document_word_score
  )) as score
from  main_query
where CHAR_LENGTH(main_query.body) > 2000
group by  main_query.title, main_query.body
order by  score desc;
end;
$$;
