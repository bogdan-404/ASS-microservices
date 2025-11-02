-- Seed 1,000 long English sentences.
-- Each sentence contains a random number of bad terms between 0 and 10 (inclusive).
-- Bad terms are read from bad_terms.enabled=true and injected verbatim (multi-word kept).
-- Neutral vocabulary builds long sentences so the classifier has realistic input.

DO $$
DECLARE
  -- neutral vocabulary to build long sentences
  vocab text[] := ARRAY[
    'the','quick','brown','fox','jumps','over','the','lazy','dog',
    'please','review','this','sample','harmless','normal','content','text',
    'trusted','source','evidence','report','analysis','context','quality','dataset',
    'moderation','filter','pipeline','queue','rabbitmq','postgres','spring','java',
    'security','compliance','metrics','monitoring','latency','throughput','backlog',
    'network','client','server','message','token','cache','storage','compute',
    'cloud','scalable','reliable','robust','simple','clean','portable','repeatable',
    'worker','batch','controller','consumer','publisher','cursor','thread','parallel',
    'integration','testing','benchmark','performance','tradeoff','design','pattern',
    'service','scaling','distributed','availability','resilience','fault','tolerance',
    'scheduling','stream','ingestion','classification','aggregation','reporting'
  ];

  bw_all       text[];   -- all enabled bad terms (lowercased), as stored in bad_terms.pattern
  bw_count     int;      -- number of bad terms
  i            int;      -- sentence counter
  n_bad        int;      -- how many bad terms to inject in this sentence (0..10)
  chosen       text[];   -- selected bad terms for this sentence
  sentence     text;     -- final sentence
  blocks       int;      -- neutral blocks = n_bad + 1
  base_len     int;      -- total neutral words across all blocks
  k            int;      -- loop index for blocks
  w            int;      -- loop index for words in a block
  words_in_block int;    -- number of neutral words per block
  pick         text;     -- chosen vocab or bad term
  idx          int;      -- random index helper
BEGIN
  -- Load all enabled bad terms; keep original spacing for phrases
  SELECT array_agg(lower(pattern)) INTO bw_all
  FROM bad_terms
  WHERE enabled = true;

  IF bw_all IS NULL OR array_length(bw_all,1) IS NULL THEN
    RAISE EXCEPTION 'bad_terms table is empty or has no enabled terms.';
  END IF;
  bw_count := array_length(bw_all,1);

  -- Optional full reset:
  -- TRUNCATE TABLE texts RESTART IDENTITY CASCADE;

  FOR i IN 1..1000 LOOP
    -- choose how many bad terms to inject (0..10 inclusive)
    n_bad := floor(random()*11)::int;

    -- select n_bad distinct bad terms
    chosen := ARRAY[]::text[];
    WHILE (n_bad > 0) AND (array_length(chosen,1) IS NULL OR array_length(chosen,1) < n_bad) LOOP
      idx := 1 + floor(random()*bw_count)::int;
      pick := bw_all[idx];
      IF NOT pick = ANY(chosen) THEN
        chosen := chosen || pick;
      END IF;
    END LOOP;

    -- Build a long sentence: interleave neutral blocks with chosen bad terms.
    sentence := '';
    blocks := n_bad + 1;
    base_len := 40 + (random()*41)::int; -- ~40..80 neutral words per sentence

    FOR k IN 1..blocks LOOP
      -- neutral block size is spread across blocks with small randomness
      words_in_block := GREATEST(4, (base_len / blocks) + (random()*6)::int);
      FOR w IN 1..words_in_block LOOP
        pick := vocab[1 + floor(random()*array_length(vocab,1))::int];
        sentence := sentence || pick || ' ';
      END LOOP;

      -- after each block except the last, inject one bad term/phrase
      IF k < blocks AND n_bad > 0 THEN
        pick := chosen[k];
        sentence := sentence || pick || ' ';
      END IF;
    END LOOP;

    sentence := trim(both from sentence) || '.';
    INSERT INTO texts(content) VALUES (sentence);
  END LOOP;
END
$$;

