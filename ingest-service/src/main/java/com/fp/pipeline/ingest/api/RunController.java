package com.fp.pipeline.ingest.api;

import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
public class RunController {
  private final JdbcTemplate jdbc;
  private final RabbitTemplate rabbit;

  public RunController(JdbcTemplate jdbc, RabbitTemplate rabbit) {
    this.jdbc = jdbc; this.rabbit = rabbit;
  }

  private void ensureSchema() {
    jdbc.execute("CREATE TABLE IF NOT EXISTS runs (" +
      "id BIGSERIAL PRIMARY KEY, " +
      "requested_count INT NOT NULL, " +
      "started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), " +
      "finished_at TIMESTAMPTZ)");
    
    jdbc.execute("CREATE TABLE IF NOT EXISTS filtered_strings (" +
      "id BIGSERIAL PRIMARY KEY, " +
      "text_id BIGINT NOT NULL REFERENCES texts(id) ON DELETE CASCADE, " +
      "run_id BIGINT REFERENCES runs(id) ON DELETE SET NULL, " +
      "filtered_content TEXT NOT NULL, " +
      "removed_bad_term_ids BIGINT[] NOT NULL, " +
      "processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW())");
    
    jdbc.execute("ALTER TABLE results ADD COLUMN IF NOT EXISTS run_id BIGINT");
    
    jdbc.execute("DO $$ " +
      "BEGIN " +
      "IF NOT EXISTS ( " +
      "SELECT 1 FROM information_schema.table_constraints " +
      "WHERE constraint_name = 'fk_results_runs' AND table_name = 'results' " +
      ") THEN " +
      "ALTER TABLE results " +
      "ADD CONSTRAINT fk_results_runs " +
      "FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE SET NULL; " +
      "END IF; " +
      "END$$");
  }

  @PostMapping("/start-run")
  public Map<String,Object> startRun() {
    var texts = jdbc.query("SELECT id, content FROM texts WHERE status='PENDING'",
      (rs, i) -> Map.of("id", rs.getLong("id"), "content", rs.getString("content")));
    int enq = 0;
    for (var t : texts) {
      Map<String,Object> msg = Map.of(
        "textId", t.get("id"),
        "content", t.get("content"),
        "ts", System.currentTimeMillis()
      );
      rabbit.convertAndSend("", "incoming_texts", msg);
      enq++;
    }
    return Map.of("enqueued", enq, "startedAt", new Date());
  }

  @GetMapping("/run")
  public Map<String,Object> run(@RequestParam(name="count", defaultValue="1000") int count) {
    ensureSchema();
    
    List<Map<String, Object>> textsList = jdbc.query(
      "SELECT id, content FROM texts ORDER BY id",
      (rs, i) -> {
        Map<String, Object> m = new HashMap<>();
        m.put("id", rs.getLong("id"));
        m.put("content", rs.getString("content"));
        return m;
      }
    );
    
    if (textsList.isEmpty()) {
      return Map.of("error", "No texts found in database");
    }
    
    int total = textsList.size();
    Long runId = jdbc.queryForObject(
      "INSERT INTO runs(requested_count) VALUES (?) RETURNING id",
      Long.class,
      count
    );
    
    long startTime = System.currentTimeMillis();
    
    for (int i = 0; i < count; i++) {
      Map<String, Object> t = textsList.get(i % total);
      Map<String, Object> msg = new HashMap<>();
      msg.put("runId", runId);
      msg.put("textId", t.get("id"));
      msg.put("content", t.get("content"));
      rabbit.convertAndSend("", "incoming_texts", msg);
    }
    
    // Block until all items are processed (with timeout)
    int processed = 0;
    long maxWaitMs = 300000; // 5 minute timeout (300 seconds)
    long startWaitTime = System.currentTimeMillis();
    int pollCount = 0;
    while (processed < count) {
      long elapsedMs = System.currentTimeMillis() - startWaitTime;
      if (elapsedMs > maxWaitMs) {
        break; // Timeout reached
      }
      
      try {
        // Adaptive polling: faster at start, slower as we approach completion
        int sleepMs = (processed < count * 0.9) ? 50 : 200;
        Thread.sleep(sleepMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      Integer cnt = jdbc.queryForObject(
        "SELECT COUNT(*) FROM results WHERE run_id = ?",
        Integer.class,
        runId
      );
      processed = cnt != null ? cnt : 0;
      pollCount++;
    }
    
    if (processed < count) {
      // Timeout reached
      return Map.of(
        "error", "Timeout waiting for processing",
        "runId", runId,
        "requestedCount", count,
        "enqueued", count,
        "processed", processed,
        "message", "Not all items were processed within timeout period"
      );
    }
    
    long durationMs = System.currentTimeMillis() - startTime;
    
    jdbc.update("UPDATE runs SET finished_at = NOW() WHERE id = ?", runId);
    
    // Calculate stats for this specific run_id
    Map<String, Object> stats = jdbc.queryForMap(
      "SELECT " +
      "  COUNT(*)::BIGINT AS total_processed, " +
      "  AVG(score)::NUMERIC(10,2) AS avg_score, " +
      "  AVG(CASE WHEN has_words THEN 1 ELSE 0 END)::NUMERIC(5,4) AS bad_words_rate " +
      "FROM results WHERE run_id = ?",
      runId
    );
    
    Map<String, Object> result = new HashMap<>();
    result.put("runId", runId);
    result.put("requestedCount", count);
    result.put("enqueued", count);
    result.put("processed", processed);
    result.put("durationMs", durationMs);
    result.put("stats", stats);
    return result;
  }
}

