-- Check filtered_strings table

-- 1. Count total filtered strings
SELECT COUNT(*) as total_filtered_strings FROM filtered_strings;

-- 2. View all filtered strings with details
SELECT 
    fs.id,
    fs.text_id,
    fs.run_id,
    LEFT(fs.filtered_content, 100) as filtered_content_preview,
    fs.removed_bad_term_ids,
    fs.processed_at,
    t.content as original_content
FROM filtered_strings fs
LEFT JOIN texts t ON fs.text_id = t.id
ORDER BY fs.processed_at DESC
LIMIT 20;

-- 3. Count filtered strings per run
SELECT 
    run_id,
    COUNT(*) as filtered_count
FROM filtered_strings
GROUP BY run_id
ORDER BY run_id DESC;

-- 4. View filtered strings for a specific run (replace 1 with your run_id)
SELECT 
    fs.id,
    fs.text_id,
    fs.run_id,
    fs.filtered_content,
    fs.removed_bad_term_ids,
    fs.processed_at
FROM filtered_strings fs
WHERE fs.run_id = 1
ORDER BY fs.id;

-- 5. Check which bad terms were removed most often
SELECT 
    unnest(removed_bad_term_ids) as bad_term_id,
    COUNT(*) as removal_count
FROM filtered_strings
GROUP BY bad_term_id
ORDER BY removal_count DESC
LIMIT 10;

