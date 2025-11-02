package com.fp.pipeline.stats.api;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
public class StatsController {
  private final JdbcTemplate jdbc;
  public StatsController(JdbcTemplate jdbc) { this.jdbc = jdbc; }

  @GetMapping("/stats")
  public Map<String,Object> stats() {
    jdbc.execute("REFRESH MATERIALIZED VIEW results_stats");
    return jdbc.queryForMap("SELECT * FROM results_stats");
  }
}

