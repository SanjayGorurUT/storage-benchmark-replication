# Remaining Work - Focused Scope (2 Weeks)

Based on the original proposal and current progress, here's the focused remaining work to complete the project.

## ✅ Already Completed

1. ✅ Workload generation with distribution validation (NDV, null ratio, sortedness, skew)
2. ✅ Synthetic data for all 6 workloads (core, bi, classic, geo, log, ml)
3. ✅ Format conversion (Parquet ↔ ORC)
4. ✅ Benchmark runner (file size, full scan, selection queries)
5. ✅ Basic visualization (three bar charts matching Figure 6 structure)
6. ✅ Docker and bare-metal support
7. ✅ Preliminary results (1K rows)

## 🎯 Core Remaining Work (Must Complete)

### 1. Scale Up to Larger Datasets
**Priority: HIGH | Time: 2-3 hours**

- [ ] Make `workload_generator.py` read row count from config (currently hardcoded to 1000)
- [ ] Support configurable scale: 100K rows (good balance of speed vs. realism)
- [ ] Update file naming to handle different scales
- [ ] Update `benchmark_runner.py` to handle different file patterns

**Why:** The proposal mentions using realistic dataset sizes. 100K rows provides good performance characteristics while remaining manageable for 2-week timeline.

### 2. Run Final Benchmarks
**Priority: HIGH | Time: 3-4 hours (execution time depends on hardware)**

- [ ] Generate 100K row workloads for all 6 workload types
- [ ] Convert to Parquet and ORC
- [ ] Run benchmarks on bare-metal (for final results)
- [ ] Ensure 10+ iterations for statistical reliability

**Why:** Need final results across all workloads to replicate Figure 6 findings.

### 3. Polish Figure 6 Visualizations
**Priority: HIGH | Time: 1-2 hours**

- [ ] Match paper's figure style (colors, labels, layout)
- [ ] Ensure workload order matches paper (bi, classic, geo, log, ml, core)
- [ ] Add proper axis labels and formatting
- [ ] Generate publication-quality PNG/PDF outputs

**Why:** The three charts are already implemented, just need formatting polish to match the paper.

### 4. Final Analysis and Write-up
**Priority: HIGH | Time: 3-4 hours**

- [ ] Analyze results to validate "no clear winner" finding
- [ ] Compare trends with reference paper (if possible)
- [ ] Document methodology and findings
- [ ] Prepare final report sections

**Why:** Core deliverable - need to show we replicated the key finding.

## 📋 Optional Enhancements (If Time Permits)

### 5. Enhanced Metrics
**Priority: LOW | Time: 1-2 hours**

- [ ] Add compression ratio (CSV size vs. compressed)
- [ ] Calculate standard deviations for error bars

### 6. Artifact Preparation
**Priority: LOW | Time: 1-2 hours**

- [ ] Create artifact README
- [ ] Document how to reproduce results
- [ ] Package Docker setup

## Implementation Plan

### Week 1 (Days 1-3)
1. **Day 1:** Scale up workload generator to 100K rows
2. **Day 2:** Generate all 6 workloads at 100K scale
3. **Day 3:** Run benchmarks on all workloads

### Week 2 (Days 4-6)
1. **Day 4:** Polish Figure 6 visualizations
2. **Day 5:** Analyze results and write findings
3. **Day 6:** Final report and artifact preparation

## Key Files to Modify

1. `workload_generator.py` - Line 138: Change `n_rows = 1000` to read from config
2. `visualizer.py` - Polish formatting to match paper style
3. `generate_preliminary_results.py` - Add scale parameter support

## Success Criteria (From Proposal)

✅ **Replication is successful if:**
- We can reproduce the general performance trends shown in Figure 6
- Results show "no clear winner" between Parquet and ORC
- Three bar charts (file size, full scan, selection latency) are generated
- All 6 workloads are benchmarked

## Estimated Total Effort

- **Core work (must complete):** ~10-13 hours
- **Optional enhancements:** ~2-4 hours
- **Total:** ~12-17 hours over 2 weeks

This is a realistic scope that focuses on the core replication goal without over-engineering.
