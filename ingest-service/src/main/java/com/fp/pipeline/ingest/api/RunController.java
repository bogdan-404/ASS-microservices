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
}

