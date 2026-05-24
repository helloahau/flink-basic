# flink-basic

Java examples extracted from **11-flink** Yuque notes (`BD20250501`).

## Layout

```
src/main/java/          Full tutorial classes (by package)
src/main/java/com/bigdata/snippets/   Short code fragments from docs
src/main/resources/     XML/properties/shell notes from tutorials
sources.json            Mapping of each saved block to source doc
```

## Build

```bash
cd /Volumes/Work/Flink/Project/basic
mvn -q compile
```

Note: many files are **tutorial snippets** copied verbatim from docs. Not every class compiles together without edits (duplicate class names are suffixed with `_2`, `_3`, etc.).

## Re-extract from Yuque

```bash
python3 extract_from_yuque.py
```

Source HTML: `/Volumes/Work/Flink/Learn/` (section **11-flink**).
